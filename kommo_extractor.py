import logging
import datetime as dt
import time
from typing import Optional

import pandas as pd
import requests
import json
from google.cloud import bigquery

from comodo_utils.auxiliar_functions import ELTExecutor
from kommo_crm.kommo_utils import KommoUtils

logger = logging.getLogger(__name__)

MAX_RETRIES = 3
RETRY_DELAY = 20
DATASET_RAW = "kommo_raw"
DLQ_TABLE = "dlq"

_SEP = "=" * 60


class KommoExtractor:
    """
    Responsável pela extração incremental dos dados do Kommo CRM
    e carregamento direto na camada RAW do BigQuery (padrão ELT).

    Princípios aplicados:
    - ELT: Python só extrai. Transformações ficam no BigQuery via SQL.
    - Incremental: checkpoint via MAX(updated_at) na camada RAW.
    - Idempotência: dimensões usam WRITE_TRUNCATE; fatos usam WRITE_APPEND
      com deduplicação por SQL posterior.
    - DLQ: registros com falha são isolados em kommo_raw.dlq para
      auditoria e reprocessamento, sem derrubar o pipeline.
    - Multi-tenant: itera sobre todas as contas do clientes.json.
    - Renovação de token sob demanda: só renova quando recebe 401 durante
      a extração, sem chamar renovar_chaves() em massa antes de tudo.
      Tokens Kommo podem ter validade de anos — renovação prévia é desnecessária
      e causava falsos erros para contas com refresh_token expirado.
    """

    def __init__(self):
        self.elt = ELTExecutor()
        self.clientes = KommoUtils.get_clientes()
        self.lista_clientes = sorted(self.clientes.keys())
        logger.info(
            f"KommoExtractor inicializado: {len(self.lista_clientes)} conta(s) carregada(s)."
        )

    # ------------------------------------------------------------------
    # HELPERS INTERNOS
    # ------------------------------------------------------------------

    def _authenticated_request(
        self,
        url: str,
        conta: str,
        max_retries: int = MAX_RETRIES,
        retry_delay: int = RETRY_DELAY,
    ) -> Optional[dict]:
        """
        Executa uma requisição GET autenticada com retry automático e
        backoff linear.

        Renovação de token sob demanda: se receber 401, tenta renovar o
        access_token via refresh_token antes de desistir. Se a renovação
        funcionar, refaz a requisição com o novo token. Se não funcionar,
        retorna erro — o token pode ter expirado definitivamente.

        :param url: URL da requisição.
        :param conta: Nome da conta (usado para renovação de token).
        :return: dict com a resposta | None se 204 | dict com 'error' em
                 falha persistente.
        """
        token_renovado = False

        for attempt in range(1, max_retries + 1):
            access_token = self.clientes[conta]['access_token']
            headers = {"Authorization": f"Bearer {access_token}"}

            try:
                response = requests.get(url, headers=headers, timeout=30)

                if response.status_code == 200:
                    return response.json()

                if response.status_code == 204:
                    return None

                # 401 — tenta renovar o token uma única vez
                if response.status_code == 401 and not token_renovado:
                    logger.warning(
                        f"[{conta}] 401 recebido — tentando renovar token..."
                    )
                    self.clientes[conta] = KommoUtils.renovar_chave_individual(
                        conta, self.clientes[conta]
                    )
                    token_renovado = True
                    # Não conta como tentativa — refaz imediatamente
                    continue

                logger.warning(
                    f"[{conta}] Tentativa {attempt}/{max_retries} — "
                    f"Status {response.status_code}: {response.text[:200]}"
                )

                # Erros 4xx (exceto 401 já tratado) não têm recuperação via retry
                if 400 <= response.status_code < 500:
                    return {"error": f"{response.status_code} - {response.reason}"}

                time.sleep(retry_delay)

            except Exception as e:
                logger.warning(f"[{conta}] Tentativa {attempt}/{max_retries} falhou: {e}")
                time.sleep(retry_delay)

        return {"error": f"[{conta}] Falha após {max_retries} tentativas — {url}"}

    def _get_checkpoint(
        self,
        table_id: str,
        conta: str,
        date_column: str = "updated_at",
    ) -> Optional[str]:
        """
        Busca o checkpoint incremental: MAX(date_column) registrado no BQ
        para uma conta específica. Usado para filtrar apenas registros
        novos/alterados desde a última execução.

        :return: String com o último timestamp | None se tabela não existe
                 ou não há registros para a conta.
        """
        sql = f"""
            SELECT MAX({date_column}) AS last_update
            FROM `{self.elt.bigquery_client.project}.{DATASET_RAW}.{table_id}`
            WHERE account_subdomain = @conta
        """
        job_config = bigquery.QueryJobConfig(
            query_parameters=[
                bigquery.ScalarQueryParameter("conta", "STRING", conta)
            ]
        )
        try:
            df = self.elt.query_bigquery(sql, table_id, job_config=job_config)
            val = df["last_update"].iloc[0]
            if pd.isnull(val):
                return None
            logger.info(f"[checkpoint] {table_id}/{conta}: {val}")
            return str(val)
        except Exception:
            logger.info(
                f"[checkpoint] Tabela {table_id} não encontrada para '{conta}' "
                "— execução full inicial."
            )
            return None

    def _send_to_dlq(
        self,
        conta: str,
        entidade: str,
        records: list,
        error: str,
    ):
        """
        Isola registros com falha na Dead Letter Queue (kommo_raw.dlq).
        O pipeline principal nunca é interrompido por causa de um registro
        problemático — ele é quarentenado aqui para auditoria e
        reprocessamento posterior.
        """
        # Garante que sempre há pelo menos um registro na DLQ
        # mesmo quando o erro ocorre antes de qualquer dado ser extraído
        if not records:
            records = [{"conta": conta, "entidade": entidade}]

        try:
            df_dlq = pd.DataFrame([
                {
                    "_ingestion_timestamp": dt.datetime.now(dt.timezone.utc).isoformat(),
                    "account_subdomain": conta,
                    "entidade": entidade,
                    "error_message": str(error)[:1000],
                    "raw_payload": str(record)[:5000],
                }
                for record in records
            ])
            self.elt.load_to_bigquery_raw(
                dataframe=df_dlq,
                dataset_id=DATASET_RAW,
                table_id=DLQ_TABLE,
                write_disposition="WRITE_APPEND",
            )
            logger.warning(
                f"[DLQ] {len(records)} registro(s) isolados → "
                f"{DATASET_RAW}.{DLQ_TABLE} [{conta}/{entidade}]"
            )
        except Exception as e:
            logger.error(f"[DLQ] Falha ao gravar na DLQ [{conta}/{entidade}]: {e}")

    def _add_metadata(self, df: pd.DataFrame, conta: str) -> pd.DataFrame:
        """
        Adiciona colunas de rastreabilidade ao DataFrame bruto antes do load.
        """
        df["account_subdomain"] = conta
        df["_ingestion_timestamp"] = dt.datetime.now(dt.timezone.utc).isoformat()
        return df

    def _paginar(
        self,
        url_inicial: str,
        conta: str,
        embedded_key: str,
    ):
        """
        Gerador que itera sobre páginas da API do Kommo.
        Busca uma página por vez via _links.next, liberando memória
        antes de carregar a próxima.

        :param url_inicial: URL da primeira página.
        :param conta: Nome da conta (usado para autenticação e renovação de token).
        :param embedded_key: Chave dentro de _embedded (ex: 'leads', 'tasks').
        :yields: (registros, num_pagina)
        :raises ValueError: Se a API retornar erro em qualquer página.
        """
        url = url_inicial
        num_pagina = 0

        while url:
            num_pagina += 1
            resultado = self._authenticated_request(url, conta)

            if resultado is None:
                break

            if "error" in resultado:
                raise ValueError(
                    f"Erro na página {num_pagina}: {resultado['error']}"
                )

            registros = resultado.get("_embedded", {}).get(embedded_key, [])
            yield registros, num_pagina

            # Próxima página via _links.next — None encerra o loop
            url = (
                resultado.get("_links", {})
                .get("next", {})
                .get("href")
            )

    # ------------------------------------------------------------------
    # DIMENSÕES — estratégia WRITE_TRUNCATE (sem incremental)
    # Justificativa: são tabelas pequenas e estáveis. Re-escrita completa
    # é mais simples e segura do que controle incremental para estas.
    # ------------------------------------------------------------------

    def extrair_contas(self):
        """
        Extrai metadados de cada conta Kommo (account info).
        Destino  : kommo_raw.contas
        Estratégia: WRITE_TRUNCATE — dimensão pequena, 1 registro por conta.
        """
        logger.info(_SEP)
        logger.info("  EXTRAÇÃO: CONTAS")
        logger.info(_SEP)
        registros = []
        falhas = []

        for conta in self.lista_clientes:
            dados = self.clientes[conta]
            url = f"https://{dados['url']}.kommo.com/api/v4/account?with=amojo_id"
            resultado = self._authenticated_request(url, conta)

            if resultado is None:
                logger.warning(f"[contas] {conta}: 204 No Content — pulando.")
                continue

            if "error" in resultado:
                logger.error(f"[contas] {conta}: {resultado['error']}")
                self._send_to_dlq(conta, "contas", [resultado], resultado["error"])
                falhas.append(conta)
                continue

            try:
                row = {
                    k: v
                    for k, v in resultado.items()
                    if not isinstance(v, (dict, list))
                }
                row["account_subdomain"] = conta
                row["_ingestion_timestamp"] = dt.datetime.now(dt.timezone.utc).isoformat()
                registros.append(row)
                logger.info(f"[contas] {conta}: ok")

            except Exception as e:
                self._send_to_dlq(conta, "contas", [resultado], str(e))
                falhas.append(conta)

        if registros:
            df = pd.DataFrame(registros)
            df = self.elt.prepare_dataframe_for_load(df)
            self.elt.load_to_bigquery_raw(
                dataframe=df,
                dataset_id=DATASET_RAW,
                table_id="contas",
                write_disposition="WRITE_TRUNCATE",
            )

        logger.info(
            f"[contas] Concluído: {len(registros)} ok | {len(falhas)} falha(s)."
        )
        logger.info(_SEP)

    def extrair_tags(self):
        """
        Extrai tags de leads por conta.
        Destino  : kommo_raw.tags
        Estratégia: WRITE_TRUNCATE — tags mudam pouco e o volume é baixo.
        """
        logger.info(_SEP)
        logger.info("  EXTRAÇÃO: TAGS")
        logger.info(_SEP)
        registros = []
        falhas = []

        for conta in self.lista_clientes:
            dados = self.clientes[conta]
            url = (
                f"https://{dados['url']}.kommo.com/api/v4/leads/tags"
                "?page=1&limit=250"
            )
            resultado = self._authenticated_request(url, conta)

            if resultado is None:
                logger.info(f"[tags] {conta}: sem tags (204).")
                continue

            if "error" in resultado:
                self._send_to_dlq(conta, "tags", [resultado], resultado["error"])
                falhas.append(conta)
                continue

            try:
                tags = resultado.get("_embedded", {}).get("tags", [])
                for tag in tags:
                    tag["account_subdomain"] = conta
                    tag["_ingestion_timestamp"] = dt.datetime.now(dt.timezone.utc).isoformat()
                registros.extend(tags)
                logger.info(f"[tags] {conta}: {len(tags)} tag(s) ok")

            except Exception as e:
                self._send_to_dlq(conta, "tags", [resultado], str(e))
                falhas.append(conta)

        if registros:
            df = pd.DataFrame(registros)
            df = self.elt.prepare_dataframe_for_load(df)
            self.elt.load_to_bigquery_raw(
                dataframe=df,
                dataset_id=DATASET_RAW,
                table_id="tags",
                write_disposition="WRITE_TRUNCATE",
            )

        logger.info(
            f"[tags] Concluído: {len(registros)} registro(s) | {len(falhas)} falha(s)."
        )
        logger.info(_SEP)

    def extrair_usuarios(self):
        """
        Extrai usuários com seus roles e groups por conta.
        Destino  : kommo_raw.usuarios
        Estratégia: WRITE_TRUNCATE
        Flatten  : _embedded (roles/groups) → colunas escalares no raw.
        """
        logger.info(_SEP)
        logger.info("  EXTRAÇÃO: USUÁRIOS")
        logger.info(_SEP)
        registros = []
        falhas = []

        for conta in self.lista_clientes:
            dados = self.clientes[conta]
            url = (
                f"https://{dados['url']}.kommo.com/api/v4/users"
                "?with=role,group&page=1&limit=250"
            )
            resultado = self._authenticated_request(url, conta)

            if resultado is None:
                logger.info(f"[usuarios] {conta}: 204 No Content.")
                continue

            if "error" in resultado:
                self._send_to_dlq(conta, "usuarios", [resultado], resultado["error"])
                falhas.append(conta)
                continue

            try:
                users = resultado.get("_embedded", {}).get("users", [])
                for user in users:
                    # 1. Troca pop por get para não destruir o JSON
                    embedded = user.get("_embedded", {}) or {}
                    for key, values in embedded.items():
                        if isinstance(values, list) and values:
                            user[f"{key}_id"] = values[0].get("id")
                            user[f"{key}_name"] = values[0].get("name", "")
                    
                    # 2. Serializa o _embedded para STRING (Igual você fez nas outras!)
                    emb = user.get("_embedded")
                    user["_embedded"] = json.dumps(emb, ensure_ascii=False) if emb else None

                    # Transforma em string Python padrão, preservando a ordem original da API!
                    if "rights" in user:
                        user["rights"] = str(user["rights"]) if user["rights"] is not None else None
                    if "_links" in user:
                        user["_links"] = str(user["_links"]) if user["_links"] is not None else None
                    # ----------------------------------

                    user["account_subdomain"] = conta
                    user["_ingestion_timestamp"] = dt.datetime.now(dt.timezone.utc).isoformat()

                registros.extend(users)
                logger.info(f"[usuarios] {conta}: {len(users)} usuário(s) ok")

            except Exception as e:
                self._send_to_dlq(conta, "usuarios", [resultado], str(e))
                falhas.append(conta)

        if registros:
            df = pd.DataFrame(registros)
            df = self.elt.prepare_dataframe_for_load(df)
            self.elt.load_to_bigquery_raw(
                dataframe=df,
                dataset_id=DATASET_RAW,
                table_id="usuarios",
                write_disposition="WRITE_TRUNCATE",
            )

        logger.info(
            f"[usuarios] Concluído: {len(registros)} registro(s) | {len(falhas)} falha(s)."
        )
        logger.info(_SEP)

    def extrair_pipelines_e_statuses(self):
        """
        Extrai pipelines e seus statuses por conta em uma única chamada de API.
        Destino  : kommo_raw.pipelines | kommo_raw.statuses
        Estratégia: WRITE_TRUNCATE para ambas.
        """
        logger.info(_SEP)
        logger.info("  EXTRAÇÃO: PIPELINES E STATUSES")
        logger.info(_SEP)
        pipelines_registros = []
        statuses_registros = []
        falhas = []

        for conta in self.lista_clientes:
            dados = self.clientes[conta]
            url = (
                f"https://{dados['url']}.kommo.com/api/v4/leads/pipelines"
                "?limit=249"
            )
            resultado = self._authenticated_request(url, conta)

            if resultado is None:
                logger.info(f"[pipelines] {conta}: 204 No Content.")
                continue

            if "error" in resultado:
                self._send_to_dlq(conta, "pipelines", [resultado], resultado["error"])
                falhas.append(conta)
                continue

            try:
                pipelines = resultado.get("_embedded", {}).get("pipelines", [])
                conta_statuses = 0

                for pipeline in pipelines:
                    pipeline_id = pipeline.get("id")
                    embedded_statuses = (
                        pipeline.pop("_embedded", {}).get("statuses", [])
                    )
                    pipeline["account_subdomain"] = conta
                    pipeline["_ingestion_timestamp"] = (
                        dt.datetime.now(dt.timezone.utc).isoformat()
                    )
                    pipelines_registros.append(pipeline)

                    for status in embedded_statuses:
                        status.pop("_links", None)
                        status["pipeline_id"] = pipeline_id
                        status["account_subdomain"] = conta
                        status["_ingestion_timestamp"] = (
                            dt.datetime.now(dt.timezone.utc).isoformat()
                        )
                        statuses_registros.append(status)
                        conta_statuses += 1

                logger.info(
                    f"[pipelines] {conta}: {len(pipelines)} pipeline(s), "
                    f"{conta_statuses} status(es) ok"
                )

            except Exception as e:
                self._send_to_dlq(conta, "pipelines", [resultado], str(e))
                falhas.append(conta)

        if pipelines_registros:
            df_p = pd.DataFrame(pipelines_registros)
            df_p = self.elt.prepare_dataframe_for_load(df_p)
            self.elt.load_to_bigquery_raw(
                dataframe=df_p,
                dataset_id=DATASET_RAW,
                table_id="pipelines",
                write_disposition="WRITE_TRUNCATE",
            )

        if statuses_registros:
            df_s = pd.DataFrame(statuses_registros)
            df_s = self.elt.prepare_dataframe_for_load(df_s)
            self.elt.load_to_bigquery_raw(
                dataframe=df_s,
                dataset_id=DATASET_RAW,
                table_id="statuses",
                write_disposition="WRITE_TRUNCATE",
            )

        logger.info(
            f"[pipelines/statuses] Concluído: {len(pipelines_registros)} pipeline(s), "
            f"{len(statuses_registros)} status(es) | {len(falhas)} falha(s)."
        )
        logger.info(_SEP)

    # ------------------------------------------------------------------
    # FATOS — estratégia incremental + paginação via _paginar()
    # ------------------------------------------------------------------

    def extrair_listas(self):
        """
        Extrai catálogos (listas) e seus elementos por conta.
        Destino: kommo_raw.listas | kommo_raw.listas_elementos
        Estratégia: WRITE_TRUNCATE - volume controlado, estrutura estável.
        """
        logger.info(_SEP)
        logger.info("  EXTRAÇÃO: LISTAS")
        logger.info(_SEP)
        listas_registros = []
        elementos_registros = []
        falhas = []
 
        for conta in self.lista_clientes:
            dados = self.clientes[conta]
            url_catalogs = (
                f"https://{dados['url']}.kommo.com/api/v4/catalogs"
                "?page=1&limit=249"
            )
            resultado = self._authenticated_request(url_catalogs, conta)
 
            if resultado is None:
                logger.info(f"[listas] {conta}: sem catálogos (204)")
                continue
 
            if "error" in resultado:
                self._send_to_dlq(conta, "listas", [resultado], resultado["error"])
                falhas.append(conta)
                continue
 
            try:
                catalogs = resultado.get("_embedded", {}).get("catalogs", [])
 
                for catalog in catalogs:
                    catalog_id = catalog.get("id")
                    catalog_name = catalog.get("name", str(catalog_id))
 
                    catalog["account_subdomain"] = conta
                    catalog["_ingestion_timestamp"] = (
                        dt.datetime.now(dt.timezone.utc).isoformat()
                    )
                    listas_registros.append(catalog)
 
                    url_elements = (
                        f"https://{dados['url']}.kommo.com/api/v4"
                        f"/catalogs/{catalog_id}/elements?page=1&limit=249"
                    )
 
                    try:
                        for pagina_elementos, num_pagina in self._paginar(
                            url_elements, conta, "elements"
                        ):
                            for element in pagina_elementos:
                                # Extrai valores flattened dos custom fields
                                for field in element.get("custom_fields_values", []) or []:
                                    values = field.get("values", [])
                                    if values:
                                        element[field["field_name"]] = values[0].get("value")
 
                                # Serializa colunas aninhadas para STRING
                                # Garante idempotência com schema existente no BI
                                cfv = element.get("custom_fields_values")
                                element["custom_fields_values"] = (
                                    json.dumps(cfv, ensure_ascii=False) if cfv else None
                                )
                                links = element.get("_links")
                                element["_links"] = (
                                    json.dumps(links, ensure_ascii=False) if links else None
                                )
                                embedded = element.get("_embedded")
                                element["_embedded"] = (
                                    json.dumps(embedded, ensure_ascii=False) if embedded else None
                                )
 
                                element["catalog_id"] = catalog_id
                                element["catalog_name"] = catalog_name
                                element["account_subdomain"] = conta
                                element["_ingestion_timestamp"] = (
                                    dt.datetime.now(dt.timezone.utc).isoformat()
                                )
                                elementos_registros.append(element)
 
                    except ValueError as e:
                        self._send_to_dlq(
                            conta, f"listas_elementos/{catalog_name}",
                            [{"catalog_id": catalog_id}], str(e)
                        )
 
                logger.info(f"[listas] {conta}: {len(catalogs)} catálogo(s) OK")
 
            except Exception as e:
                self._send_to_dlq(conta, "listas", [resultado], str(e))
                falhas.append(conta)
 
        if listas_registros:
            df_l = pd.DataFrame(listas_registros)
            df_l = self.elt.prepare_dataframe_for_load(df_l)
            self.elt.load_to_bigquery_raw(
                dataframe=df_l,
                dataset_id=DATASET_RAW,
                table_id="listas",
                write_disposition="WRITE_TRUNCATE",
            )
 
        if elementos_registros:
            df_e = pd.DataFrame(elementos_registros)
            df_e = self.elt.prepare_dataframe_for_load(df_e)
            self.elt.load_to_bigquery_raw(
                dataframe=df_e,
                dataset_id=DATASET_RAW,
                table_id="listas_elementos",
                write_disposition="WRITE_TRUNCATE",
            )
 
        logger.info(
            f"[listas] Concluído: {len(listas_registros)} lista(s), "
            f"{len(elementos_registros)} elemento(s) | {len(falhas)} falha(s)"
        )
        logger.info(_SEP)


    def extrair_tarefas(self):
        """
        Extrai tarefas com paginação e extração incremental por updated_at.
        Destino: kommo_raw.tarefas
        Estratégia: WRITE_APPEND incremental.
        """
        logger.info(_SEP)
        logger.info("  EXTRAÇÃO: TAREFAS")
        logger.info(_SEP)
        falhas = []
        total_registros = 0

        for conta in self.lista_clientes:
            dados = self.clientes[conta]
            checkpoint = self._get_checkpoint("tarefas", conta, "updated_at")

            filtro_data = ""
            if checkpoint:
                ts = int(float(checkpoint))
                filtro_data = f"&filter[updated_at][from]={ts}"
                logger.info(f"[tarefas] {conta}: incremental a partir de {checkpoint}")
            else:
                logger.info(f"[tarefas] {conta}: sem checkpoint - full load inicial")

            url_inicial = (
                f"https://{dados['url']}.kommo.com/api/v4/tasks"
                f"?page=1&limit=249{filtro_data}"
            )

            registros_conta = []

            try:
                for pagina_tarefas, num_pagina in self._paginar(
                    url_inicial, conta, "tasks"
                ):
                    for task in pagina_tarefas:
                        result = task.pop("result", None) or {}
                        if isinstance(result, dict):
                            for k, v in result.items():
                                task[f"result_{k}"] = v

                        task["account_subdomain"] = conta
                        task["_ingestion_timestamp"] = (
                            dt.datetime.now(dt.timezone.utc).isoformat()
                        )
                        registros_conta.append(task)

                if registros_conta:
                    df = pd.DataFrame(registros_conta)
                    df = self.elt.prepare_dataframe_for_load(df)
                    self.elt.load_to_bigquery_raw(
                        dataframe=df,
                        dataset_id=DATASET_RAW,
                        table_id="tarefas",
                        write_disposition="WRITE_APPEND",
                    )
                    total_registros += len(registros_conta)
                    logger.info(
                        f"[tarefas] {conta}: {len(registros_conta)} tarefa(s) carregada(s)"
                    )
                else:
                    logger.info(f"[tarefas] {conta}: sem novas tarefas desde {checkpoint}")

            except Exception as e:
                logger.error(f"[tarefas] {conta}: {e}")
                self._send_to_dlq(conta, "tarefas", [], str(e))
                falhas.append(conta)

        logger.info(
            f"[tarefas] Concluído: {total_registros} registro(s) | {len(falhas)} falha(s)."
        )
        logger.info(_SEP)

    
    def extrair_leads(self):
        """
        Extrai leads com paginação e extração incremental por updated_at.
        Destino: kommo_raw.leads
        Estratégia: WRITE_APPEND incremental.
 
        Deleções são rastreadas via tabela de eventos (tipo 'lead_deleted'),
        não pelo campo is_deleted — a API do Kommo não retorna leads deletados
        no endpoint de listagem, independente do parâmetro with=is_deleted.
        """
        logger.info(_SEP)
        logger.info("  EXTRAÇÃO: LEADS")
        logger.info(_SEP)
        falhas = []
        total_registros = 0

        for conta in self.lista_clientes:
            dados = self.clientes[conta]
            checkpoint = self._get_checkpoint("leads", conta, "updated_at")
 
            filtro_data = ""
            if checkpoint:
                ts = int(float(checkpoint))
                filtro_data = f"&filter[updated_at][from]={ts}"
                logger.info(f"[leads] {conta}: incremental a partir de {checkpoint}")
            else:
                logger.info(f"[leads] {conta}: sem checkpoint - full load inicial")
 
            url_inicial = (
                f"https://{dados['url']}.kommo.com/api/v4/leads"
                f"?with=source_id,contacts,loss_reason"
                f"&page=1&limit=250{filtro_data}"
            )
 
            registros_conta = []
 
            try:
                for pagina_leads, num_pagina in self._paginar(
                    url_inicial, conta, "leads"
                ):
                    for lead in pagina_leads:
                        # Extrai valores flattened dos custom fields
                        for field in lead.get("custom_fields_values", []) or []:
                            values = field.get("values", [])
                            if values:
                                lead[field["field_name"]] = values[0].get("value")
 
                        # Serializa custom_fields_values para STRING
                        # Garante idempotência com schema existente no BI
                        cfv = lead.get("custom_fields_values")
                        lead["custom_fields_values"] = (
                            json.dumps(cfv, ensure_ascii=False) if cfv else None
                        )
 
                        # Extrai _embedded e serializa para STRING
                        embedded = lead.pop("_embedded", {}) or {}
                        for key, values in embedded.items():
                            if not isinstance(values, list) or not values:
                                continue
 
                            if key == "tags":
                                tag_comodo = next(
                                    (
                                        t for t in values
                                        if isinstance(t.get("name"), str)
                                        and "[CO]" in t["name"]
                                    ),
                                    values[0],
                                )
                                lead["tags_id"] = tag_comodo.get("id")
                                lead["tags_name"] = tag_comodo.get("name")
                            else:
                                lead[f"{key}_id"] = values[0].get("id")
                                # Preserva o name para todos os embedded (ex: loss_reason_name)
                                # Compatível com o comportamento do script original
                                name = values[0].get("name")
                                if name is not None:
                                    lead[f"{key}_name"] = name
 
                        # Serializa _links para STRING
                        links = lead.get("_links")
                        lead["_links"] = (
                            json.dumps(links, ensure_ascii=False) if links else None
                        )
 
                        lead["account_subdomain"] = conta
                        lead["_ingestion_timestamp"] = (
                            dt.datetime.now(dt.timezone.utc).isoformat()
                        )
                        registros_conta.append(lead)
 
                    logger.info(
                        f"[leads] {conta}: página {num_pagina} - "
                        f"{len(registros_conta)} lead(s) acumulado(s)"
                    )
 
                if registros_conta:
                    df = pd.DataFrame(registros_conta)
                    df = self.elt.prepare_dataframe_for_load(df)
                    self.elt.load_to_bigquery_raw(
                        dataframe=df,
                        dataset_id=DATASET_RAW,
                        table_id="leads",
                        write_disposition="WRITE_APPEND",
                    )
                    total_registros += len(registros_conta)
                    logger.info(
                        f"[leads] {conta}: {len(registros_conta)} lead(s) carregado(s)"
                    )
                else:
                    logger.info(f"[leads] {conta}: sem novos leads desde {checkpoint}")
 
            except Exception as e:
                logger.error(f"[leads] {conta}: {e}")
                self._send_to_dlq(conta, "leads", [], str(e))
                falhas.append(conta)
 
        logger.info(
            f"[leads] Concluído: {total_registros} registro(s) | {len(falhas)} falha(s)."
        )
        logger.info(_SEP)


    def extrair_eventos(self):
        """
        Extrai eventos com paginação e janela temporal por created_at.
        Destino: kommo_raw.eventos
        Estratégia: WRITE_APPEND com janela fixa de 2 semanas.

        Eventos são imutáveis — a janela fixa com deduplicação SQL posterior
        é mais segura do que checkpoint estrito, pois eventos recentes podem
        chegar atrasados na API do Kommo.

        Também é a fonte de verdade para deleções de leads:
        eventos do tipo 'lead_deleted' são usados pela view SQL para
        excluir leads deletados sem depender do campo is_deleted.
        """
        logger.info(_SEP)
        logger.info("  EXTRAÇÃO: EVENTOS")
        logger.info(_SEP)
        falhas = []
        total_registros = 0

        agora = dt.datetime.now(dt.timezone.utc)
        inicio_janela = agora - dt.timedelta(weeks=2)
        ts_inicio = int(inicio_janela.timestamp())
        ts_fim = int(agora.timestamp())

        logger.info(
            f"[eventos] Janela: {inicio_janela.date()} -> {agora.date()}"
        )

        for conta in self.lista_clientes:
            dados = self.clientes[conta]
            url_inicial = (
                f"https://{dados['url']}.kommo.com/api/v4/events"
                f"?with=contact_name,lead_name,company_name"
                f"&page=1&limit=100"
                f"&filter[created_at][from]={ts_inicio}"
                f"&filter[created_at][to]={ts_fim}"
            )

            registros_conta = []

            try:
                for pagina_eventos, num_pagina in self._paginar(
                    url_inicial, conta, "events"
                ):
                    for event in pagina_eventos:
                        # Lê value_before, extrai flattened e serializa para STRING
                        value_before = event.get("value_before", None) or []
                        if value_before and isinstance(value_before, list):
                            for k, v in value_before[0].items():
                                event[f"{k}_before"] = v
                        event["value_before"] = (
                            json.dumps(value_before, ensure_ascii=False)
                            if value_before else None
                        )

                        # Lê value_after, extrai flattened e serializa para STRING
                        value_after = event.get("value_after", None) or []
                        if value_after and isinstance(value_after, list):
                            for k, v in value_after[0].items():
                                event[f"{k}_after"] = v
                        event["value_after"] = (
                            json.dumps(value_after, ensure_ascii=False)
                            if value_after else None
                        )

                        # Extrai _embedded e serializa para STRING
                        embedded = event.pop("_embedded", {}) or {}
                        for key, values in embedded.items():
                            if isinstance(values, list) and values:
                                event[f"{key}_id"] = values[0].get("id")
                                event[f"{key}_name"] = values[0].get("name", "")
                        event["_embedded"] = (
                            json.dumps(embedded, ensure_ascii=False) if embedded else None
                        )

                        # Serializa _links para STRING
                        links = event.get("_links")
                        event["_links"] = (
                            json.dumps(links, ensure_ascii=False) if links else None
                        )

                        event["account_subdomain"] = conta
                        event["_ingestion_timestamp"] = (
                            dt.datetime.now(dt.timezone.utc).isoformat()
                        )
                        registros_conta.append(event)

                if registros_conta:
                    df = pd.DataFrame(registros_conta)
                    df = self.elt.prepare_dataframe_for_load(df)
                    self.elt.load_to_bigquery_raw(
                        dataframe=df,
                        dataset_id=DATASET_RAW,
                        table_id="eventos",
                        write_disposition="WRITE_APPEND",
                    )
                    total_registros += len(registros_conta)
                    logger.info(
                        f"[eventos] {conta}: {len(registros_conta)} evento(s) carregado(s)"
                    )
                else:
                    logger.info(f"[eventos] {conta}: sem eventos na janela.")

            except Exception as e:
                logger.error(f"[eventos] {conta}: {e}")
                self._send_to_dlq(conta, "eventos", [], str(e))
                falhas.append(conta)

        logger.info(
            f"[eventos] Concluído: {total_registros} registro(s) | {len(falhas)} falha(s)"
        )
        logger.info(_SEP)


def main():
    extractor = KommoExtractor()

    # Dimensões - WRITE_TRUNCATE, sem incremental
    extractor.extrair_contas()
    # extractor.extrair_tags()
    # extractor.extrair_usuarios()
    # extractor.extrair_pipelines_e_statuses()
    # extractor.extrair_listas()

    # Fatos - incrementais, WRITE_APPEND
    # extractor.extrair_tarefas()
    # extractor.extrair_leads()
    extractor.extrair_eventos()

if __name__ == "__main__":
    main()
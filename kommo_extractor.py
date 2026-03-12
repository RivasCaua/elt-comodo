import logging
import datetime as dt
import time
from typing import Optional

import pandas as pd
import requests

from comodo_utils.auxiliar_functions import ELTExecutor
from kommo_crm.kommo_utils import KommoUtils

logger = logging.getLogger(__name__)

MAX_RETRIES = 3
RETRY_DELAY = 20
DATASET_RAW = "kommo_raw"
DLQ_TABLE = "dlq"


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
        access_token: str,
        max_retries: int = MAX_RETRIES,
        retry_delay: int = RETRY_DELAY,
    ) -> Optional[dict]:
        """
        Executa uma requisição GET autenticada com retry automático e
        backoff linear.

        :return: dict com a resposta | None se 204 | dict com 'error' em
                 falha persistente.
        """
        headers = {"Authorization": f"Bearer {access_token}"}

        for attempt in range(1, max_retries + 1):
            try:
                response = requests.get(url, headers=headers, timeout=30)

                if response.status_code == 200:
                    return response.json()

                if response.status_code == 204:
                    return None

                logger.warning(
                    f"Tentativa {attempt}/{max_retries} — "
                    f"Status {response.status_code}: {response.text[:200]}"
                )

                # Erros 4xx não têm recuperação via retry
                if 400 <= response.status_code < 500:
                    return {"error": f"{response.status_code} - {response.reason}"}

                time.sleep(retry_delay)

            except Exception as e:
                logger.warning(f"Tentativa {attempt}/{max_retries} falhou: {e}")
                time.sleep(retry_delay)

        return {"error": f"Falha após {max_retries} tentativas — {url}"}

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

        :return: String com a última data (ISO) | None se tabela não existe
                 ou não há registros para a conta.
        """
        sql = f"""
            SELECT MAX({date_column}) AS last_update
            FROM `{self.elt.bigquery_client.project}.{DATASET_RAW}.{table_id}`
            WHERE account_subdomain = '{conta}'
        """
        try:
            df = self.elt.query_bigquery(sql, table_id)
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

        :param conta: Subdomain da conta Kommo.
        :param entidade: Nome da entidade (ex: 'leads', 'eventos').
        :param records: Lista de registros que falharam.
        :param error: Mensagem de erro associada.
        """
        if not records:
            return
        try:
            df_dlq = pd.DataFrame([
                {
                    "ingestion_timestamp": dt.datetime.now(dt.timezone.utc).isoformat(),
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
        Adiciona colunas de rastreabilidade ao DataFrame bruto antes do
        load. Permite auditoria de ingestão e suporte ao incremental.

        Colunas adicionadas:
        - account_subdomain: identifica a conta de origem (multi-tenant)
        - _ingestion_timestamp: momento exato do carregamento no BQ
        """
        df["account_subdomain"] = conta
        df["_ingestion_timestamp"] = dt.datetime.now(dt.timezone.utc).isoformat()
        return df

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
        logger.info("=== Iniciando extração: contas ===")
        registros = []
        falhas = []

        for conta in self.lista_clientes:
            dados = self.clientes[conta]
            url = (
                f"https://{dados['url']}.kommo.com/api/v4/account?with=amojo_id"
            )
            resultado = self._authenticated_request(url, dados["access_token"])

            if resultado is None:
                logger.warning(f"[contas] {conta}: 204 No Content — pulando.")
                continue

            if "error" in resultado:
                logger.error(f"[contas] {conta}: {resultado['error']}")
                self._send_to_dlq(conta, "contas", [resultado], resultado["error"])
                falhas.append(conta)
                continue

            try:
                # Mantém apenas campos escalares — sem dicts/lists aninhados
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

    def extrair_tags(self):
        """
        Extrai tags de leads por conta.
        Destino  : kommo_raw.tags
        Estratégia: WRITE_TRUNCATE — tags mudam pouco e o volume é baixo.
        """
        logger.info("=== Iniciando extração: tags ===")
        registros = []
        falhas = []

        for conta in self.lista_clientes:
            dados = self.clientes[conta]
            url = (
                f"https://{dados['url']}.kommo.com/api/v4/leads/tags"
                "?page=1&limit=250"
            )
            resultado = self._authenticated_request(url, dados["access_token"])

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

    def extrair_usuarios(self):
        """
        Extrai usuários com seus roles e groups por conta.
        Destino  : kommo_raw.usuarios
        Estratégia: WRITE_TRUNCATE
        Flatten  : _embedded (roles/groups) → colunas escalares no raw.
        """
        logger.info("=== Iniciando extração: usuários ===")
        registros = []
        falhas = []

        for conta in self.lista_clientes:
            dados = self.clientes[conta]
            url = (
                f"https://{dados['url']}.kommo.com/api/v4/users"
                "?with=role,group&page=1&limit=250"
            )
            resultado = self._authenticated_request(url, dados["access_token"])

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
                    # Flatten _embedded: { roles: [{id, name}], groups: [...] }
                    embedded = user.pop("_embedded", {}) or {}
                    for key, values in embedded.items():
                        if isinstance(values, list) and values:
                            user[f"{key}_id"] = values[0].get("id")
                            user[f"{key}_name"] = values[0].get("name", "")

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

    def extrair_pipelines_e_statuses(self):
        """
        Extrai pipelines e seus statuses por conta em uma única chamada de API.
        Destino  : kommo_raw.pipelines | kommo_raw.statuses
        Estratégia: WRITE_TRUNCATE para ambas.
        Flatten  : statuses extraídos do _embedded de cada pipeline,
                   mantendo a referência pipeline_id.
        """
        logger.info("=== Iniciando extração: pipelines e statuses ===")
        pipelines_registros = []
        statuses_registros = []
        falhas = []

        for conta in self.lista_clientes:
            dados = self.clientes[conta]
            url = (
                f"https://{dados['url']}.kommo.com/api/v4/leads/pipelines"
                "?limit=249"
            )
            resultado = self._authenticated_request(url, dados["access_token"])

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

                    # Extrai statuses antes de remover o _embedded do pipeline
                    embedded_statuses = (
                        pipeline.pop("_embedded", {}).get("statuses", [])
                    )

                    pipeline["account_subdomain"] = conta
                    pipeline["_ingestion_timestamp"] = (
                        dt.datetime.now(dt.timezone.utc).isoformat()
                    )
                    pipelines_registros.append(pipeline)

                    for status in embedded_statuses:
                        # Remove links desnecessários no raw
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

    # ------------------------------------------------------------------
    # FATOS — estratégia incremental + paginação via _paginar()
    # ------------------------------------------------------------------

    def extrair_listas(self):
        """
        Extrai catálogos (listas) e seus elementos por conta
        Destino: kommo_raw.listas | kommo_raw.listas_elementos
        Estratégia: WRITE_TRUNCATE - volume controlado, estrutura estável.

        A extração é em dois níveis:
        1. Catalogs: Lista de catálogos da conta
        2. Elements: para cada catálogo, busca seus elementos com paginação

        Flatten: custom_fields_values dos elementos são promovidos a colunas
        escalares diretamente no raw, preservando o field_name como nome da 
        coluna
        """

        logger.info("=== Iniciando extração: listas ===")
        listas_registros = []
        elementos_registros = []
        falhas = []

        for conta in self.lista_clientes:
            dados = self.clientes[conta]
            url_catalogs = (
                f"https://{dados['url']}.kommo.com/api/v4/catalogs"
                "?page=1&limit=249"
            )
            resultado = self._authenticated_request(url_catalogs, dados["access_token"])

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

                    # busca elementos do catálogo com paginação
                    url_elements = (
                        f"https://{dados['url']}.kommo.com/api/v4"
                        f"/catalogs/{catalog_id}/elements?page=1&limit=249"
                    )

                    try:
                        for pagina_elementos, num_pagina in self._paginar(
                            url_elements, dados["access_token"], "elements"
                        ):
                            for element in pagina_elementos:
                                # Flatten: custom_fields_values -> colunas escalares
                                for field in element.pop("custom_fields_values", []) or []:
                                    values = field.get("values", [])
                                    if values:
                                        element[field["field_name"]] = values[0].get("value")

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

                logger.info(
                    f"[listas] {conta}: {len(catalogs)} catálogo(s) OK"
                )

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
            f"[listas] Concluído: {len(listas_registros)} lista(s)"
            f"{len(elementos_registros)} elemento(s) | {len(falhas)} falha(s)"
        )

    
    def extrair_tarefas(self):
        """
        Extrai tarefas com paginação e extração incremental por updated_at.
        Destino: kommo_raw.tarefas
        Estratégia: WRITE_APPEND incremental - só puxa tarefas criadas ou alteradas desde o último checkpoint registrado no BQ

        Flatten: o campo 'result' (dicionário) é promovido a colunas escalarem com prefixo 'result_' para evitar campos aninhados no raw

        Na ausência de checkpoing (primeira execução), faz full load
        """

        logger.info("=== Iniciando extração: tarefas ===")
        falhas = []
        total_registros = 0

        for conta in self.lista_clientes:
            dados = self.clientes[conta]

            # Checkpoint incremental: busca a última updated_at registrada
            checkpoint = self._get_checkpoint("tarefas", conta, "updated_at")

            filtro_data = ""
            if checkpoint:
                ts = int(dt.datetime.fromisoformat(checkpoint).timestamp())
                filtro_data = f"&filter[updated_at][from]={ts}"
                logger.info(f"[tarefas] {conta}: incremental a partir de {checkpoint}")
            else:
                logger.info(f"[tarefas] {conta}: sem checkpoint - full load inicial")

            url_inicial = (
                f"https://{dados['url']}.kommo.com/api/v4/tasks"
                f"?page=1&limit=249{filtro_data}"
            )

            registros_contas = []

            try:
                for pagina_tarefas, num_paginas in self._paginar(
                    url_inicial, dados["access_token"], "tasks"
                ):
                    
                    for task in pagina_tarefas:
                        # Flatten: result dict -> colunas result_*
                        result = task.pop("result", None) or {}
                        if isinstance(result, dict):
                            for k, v in result.items():
                                task[f"result_{k}"] = v 
                        
                        task["account_subdomain"] = conta
                        task["_ingestion_timestamp"] = (
                            dt.datetime.now(dt.timezone.utc).isoformat()
                        )
                        registros_contas.append(task)
                
                if registros_contas:
                    df = pd.DataFrame(registros_contas)
                    df = self.elt.prepare_dataframe_for_load(df)
                    self.elt.load_to_bigquery_raw(
                        dataframe=df,
                        dataset_id=DATASET_RAW,
                        table_id="tarefas",
                        write_disposition="WRITE_APPEND",
                    )
                    total_registros += len(registros_contas)
                    logger.info(
                        f"[tarefas] {conta}: {len(registros_contas)} tarefa(s) carregada(s)"
                    )

                else: 
                    logger.info(f"[tarefas] {conta}: sem novas tarefas desde {checkpoint}")

            except (ValueError, Exception) as e:
                self._send_to_dlq(conta, "tarefas", [], str(e))
                falhas.append(conta)

        logger.info(
            f"[tarefas] Concluído: {total_registros} registro(s) | {len(falhas)} falha(s)."
        )


    def extrair_leads(self):
        """
        Extrai leads com paginação e extração incremental por updated_at.
        Destino: kommo_raw.leads
        Estratégia: WRITE_APPEND incremental - resolve o OOM ao eliminar o Full Load que causava o crash da VM

        Flatten aplicado no raw:
        - custom_fields_values -> colunas escalares pelo field_name
        - _embedded.tags -> tags_id / tags_name (tag Comodo identificada pelo prefixo [CO], preservando lógica original)
        - _embedded.contacts -> contracts_id

        Na ausência de checkpoint (primeira execução), faz um Full Load completo.
        O load é feito por conta individualmente para limitar o uso excessivo de memória
        """
        logger.info("=== Iniciando extração: leads ===")
        falhas = []
        total_registros = 0

        for conta in self.lista_clientes:
            dados = self.clientes[conta]

            #checkpoint incremental
            checkpoint = self._get_checkpoint("leads", conta, "updated_at")
            
            filtro_data = ""
            if checkpoint:
                ts = int(dt.datetime.fromisoformat(checkpoint).timestamp())
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
                    url_inicial, dados["access_token"], "leads"
                ):
                    for lead in pagina_leads:
                        # Flatten: custom_field_values -> colunas escalares
                        for field in lead.pop("custom_fields_values", []) or []:
                            values = field.get("values", [])
                            if values:
                                lead[field["field_name"]] = values[0].get("value")
                        
                        # Flatten: _embedded -> tags_id, contacts_id
                        embedded = lead.pop("_embedded", {}) or {}
                        for key, values in embedded.items():
                            if not isinstance(values, list) or not values:
                                continue 

                            if key == "tags":
                                # Busca tag Comodo ([CO]) lógica preservada do original
                                tag_comodo = next(
                                    (
                                        t for t in values 
                                        if isinstance(t.get("name"), str)
                                        and "[CO]" in t["name"]
                                    ),
                                    values[0], # fallback: primeira tag
                                )
                                lead["tags_id"] = tag_comodo.get("id")
                                lead["tags_name"] = tag_comodo.get("name")
                            else:
                                lead[f"{key}_id"] = values[0].get("id")

                        lead.pop("_links", None)
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
                        write_disposition="WRITE_APPEND"
                    )

                    total_registros += len(registros_conta)
                    logger.info(
                        f"[leads] {conta}: {len(registros_conta)} lead(s) carregado(s)"
                    )

                else: 
                    logger.info(f"[leads] {conta}: sem novos leads desde {checkpoint}")
                
            except (ValueError, Exception) as e:
                self._send_to_dlq(conta, "leads", [], str(e))
                falhas.append(conta)

        logger.info(
            f"[leads] concluído: {total_registros} registro(s) | {len(falhas)} falha(s)."
        )

    
    def extrair_eventos(self):
        """
        Extrai eventos com paginação e janela temporal por created_at
        Destino: kommo_raw.eventos
        Estratégia: WRITE_APPEND com janela fixa de 2 semanas

        Eventos são imutáveis por natureza - um evento criado nunca muda
        Por isso usamos o created_at como fronteira, não o updated_at

        Não usamos checkpoint de MAX(created_at) intencionalmente:
        eventos muito recentes podem chegar atrasados na API do kommo
        então a janela fixa com WRITE_APPEND + deduplicação por SQL
        posterior é mais segura do que um checkpoint estrito.

        Flatten aplicado:
        - value_before / value_after -> colunas *_before e *_after 
        - _embedded -> entity_id, entity_name por tipo de entidade
        """

        logger.info("=== Iniciando extração: eventos ===")
        falhas = []
        total_registros = 0

        agora = dt.datetime.now(dt.timezone.utc)
        inicio_janela = agora - dt.timedelta(weeks=2)
        ts_inicio = int(inicio_janela.timestamp())
        ts_fim = int(agora.timestamp())

        logger.info(
            f"[evento] Janela: {inicio_janela.date()} -> {agora.date()}"
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
                for pagina_enventos, num_pagina in self._paginar(
                    url_inicial, dados["access_token"], "events"
                ):
                    for event in pagina_enventos:
                        # Flatten: value_before -> colunas *_before
                        value_before = event.pop("value_before", None) or []
                        if value_before and isinstance(value_before, list):
                            for k, v in value_before[0].items():
                                event[f"{k}_before"] = v

                        # Flatten: value_after -> coluna *_after
                        value_after = event.pop("value_after", None) or []
                        if value_after and isinstance(value_after, list):
                            for k, v in value_after[0].items():
                                event[f"{k}_after"] = v

                        # Flatten: _embedded -> entity_id, entity_name
                        embedded = event.pop("_embedded", {}) or {}
                        for key, values in embedded.items():
                            if isinstance(values, list) and values:
                                event[f"{key}_id"] = values[0].get("id")
                                event[f"{key}_name"] = values[0].get("name", "")

                        event.pop("_links", None)
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
                        f"[events] {conta}: {len(registros_conta)} evento(s) carregado(s)"
                    )

                else: 
                    logger.info(f"[eventos] {conta}: sem eventos na janela.")

            except (ValueError, Exception) as e:
                self._send_to_dlq(conta, "eventos", [], str(e))
                falhas.append(conta)

        logger.info(
            f"[eventos] Concluído: {total_registros} registro(s) | {len(falhas)} falha(s)"
        )


def main():
    KommoUtils.renovar_chaves()

    extractor = KommoExtractor()

    # Dimensões - WRITE_TRUNCATE, sem incremental
    extractor.extrair_contas()
    extractor.extrair_tags()
    extractor.extrair_usuarios()
    extractor.extrair_pipelines_e_statuses()
    extractor.extrair_listas()

    # Fatos - incrementais, WRITE_APPEND
    extractor.extrair_tarefas()
    extractor.extrair_leads()
    extractor.extrair_eventos()


if __name__ == "__main__":
    main()
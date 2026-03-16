import os
import re
import logging
import unicodedata
import datetime as dt

import pandas as pd
from dotenv import load_dotenv
from google.cloud import bigquery
from google.oauth2 import service_account

# Carrega variáveis do .env
load_dotenv()

# Logger centralizado
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class ELTExecutor:
    """
    Classe utilitária para operações de ELT (Extract → Load → Transform):

    - EXTRACT: Extração de dados brutos via API (responsabilidade dos scripts de cada fonte)
    - LOAD: Carregamento dos dados brutos no BigQuery (camada RAW), sem transformações de negócio
    - TRANSFORM: Transformações realizadas via SQL diretamente no BigQuery

    Métodos de preparação técnica (remoção de acentos, colunas duplicadas) são permitidos
    apenas para garantir compatibilidade com o BigQuery antes do load.
    """

    def __init__(self):
        """
        Inicializa o ELTExecutor carregando credenciais via .env.
        """
        bq_credentials_path = os.getenv('GCP_CREDENTIALS_BQ')

        if not bq_credentials_path:
            raise EnvironmentError(
                "Variável GCP_CREDENTIALS_BQ não encontrada no .env"
            )

        self.credentials_path = bq_credentials_path
        self.credentials = service_account.Credentials.from_service_account_file(
            bq_credentials_path
        )
        self.bigquery_client = bigquery.Client(
            credentials=self.credentials,
            project=os.getenv('GCP_PROJECT_ID', 'dw-comodo')
        )
        logger.info("ELTExecutor inicializado com sucesso.")

    # ------------------------------------------------------------------
    # UTILITÁRIOS DE DATA
    # ------------------------------------------------------------------

    def get_end_of_next_month(self) -> dt.date:
        """
        Retorna o último dia do próximo mês.
        Utilizado para definir janelas de extração nas APIs.
        :return: Data correspondente ao último dia do próximo mês.
        """
        today = dt.date.today()
        next_month = today.replace(day=28) + dt.timedelta(days=4)
        return next_month - dt.timedelta(days=next_month.day)

    # ------------------------------------------------------------------
    # LOAD — BIGQUERY ESCRITA (camada RAW)
    # ------------------------------------------------------------------

    def ensure_dataset_exists(self, dataset_id: str, location: str = 'us-central1'):
        '''
        Garante que o dataset exista no Bigquery.
        Se não existir, cria um novo com o nome que foi setado.

        :param dataset_id: Nome do dataset
        :param location: Localização do dataset
        '''
        dataset_ref = f"{self.bigquery_client.project}.{dataset_id}"
        try:
            self.bigquery_client.get_dataset(dataset_ref)
            logger.info(f"Dataset já existe: {dataset_id}")
        except Exception:
            dataset = bigquery.Dataset(dataset_ref)
            dataset.location = location
            self.bigquery_client.create_dataset(dataset)
            logger.info(f"Dataset Criado: {dataset_id}")

    def load_to_bigquery_raw(
        self,
        dataframe: pd.DataFrame,
        dataset_id: str,
        table_id: str,
        schema: list = None,
        write_disposition: str = bigquery.WriteDisposition.WRITE_APPEND,
        chunk_size: int = 10_000
    ):
        """
        Carrega dados brutos no BigQuery (camada RAW).
        Segue o paradigma ELT: os dados chegam sem transformações de negócio.
        O envio é feito em chunks para evitar estouro de memória (OOM).

        ALLOW_FIELD_ADDITION habilitado para suportar contas com campos
        customizados diferentes — o BigQuery aceita novos campos automaticamente
        sem rejeitar o load.

        :param dataframe: DataFrame com dados brutos extraídos da API.
        :param dataset_id: Nome do dataset RAW (ex: "kommo_raw").
        :param table_id: Nome da tabela (ex: "leads").
        :param schema: Lista de SchemaField para controle explícito de tipos.
        :param write_disposition: WRITE_APPEND (acrescenta) ou WRITE_TRUNCATE (substitui).
        :param chunk_size: Quantidade de linhas por chunk (padrão: 10.000).
        """
        self.ensure_dataset_exists(dataset_id)

        full_table_path = f"{dataset_id}.{table_id}"

        job_config = bigquery.LoadJobConfig(
            write_disposition=write_disposition,
            schema=schema if schema else [],
            # Permite adicionar novos campos automaticamente quando contas
            # possuem campos customizados que outras não têm.
            # Sem isso, o BQ rejeita qualquer load com colunas novas.
            schema_update_options=[
                bigquery.SchemaUpdateOption.ALLOW_FIELD_ADDITION
            ],
        )

        try:
            total_chunks = (len(dataframe) // chunk_size) + 1
            for i in range(0, len(dataframe), chunk_size):
                chunk = dataframe.iloc[i:i + chunk_size]
                job = self.bigquery_client.load_table_from_dataframe(
                    chunk, full_table_path, job_config=job_config
                )
                job.result()
                logger.info(
                    f"Chunk {i // chunk_size + 1}/{total_chunks} carregado: "
                    f"{len(chunk)} registros → {full_table_path}"
                )

            logger.info(
                f"Load concluído: {len(dataframe)} registros → {full_table_path}"
            )

        except Exception as e:
            logger.error(f"Erro no load para BigQuery [{full_table_path}]: {e}")
            raise

    # ------------------------------------------------------------------
    # EXTRACT — BIGQUERY LEITURA (suporte à extração incremental)
    # ------------------------------------------------------------------

    def query_bigquery(self, sql: str, table_name: str) -> pd.DataFrame:
        """
        Executa uma consulta SQL no BigQuery.
        Utilizado principalmente para buscar checkpoints de extração incremental
        (ex: última data de atualização registrada na camada RAW).

        :param sql: Consulta SQL a ser executada.
        :param table_name: Nome da tabela (usado apenas para logging).
        :return: DataFrame com os resultados.
        """
        try:
            logger.info(f"Consultando BigQuery: {table_name}")
            df = self.bigquery_client.query(sql).to_dataframe()
            logger.info(
                f"Consulta concluída: {len(df)} registros retornados de {table_name}"
            )
            return df
        except Exception as e:
            logger.error(f"Erro na consulta BigQuery [{table_name}]: {e}")
            raise

    # ------------------------------------------------------------------
    # PREPARAÇÃO TÉCNICA PARA LOAD — não são transformações de negócio
    # ------------------------------------------------------------------

    def remove_accents_and_special_chars(self, input_str: str) -> str:
        """
        Remove acentos e caracteres especiais de uma string.
        Necessário para compatibilidade com nomes de colunas no BigQuery.

        :param input_str: String original.
        :return: String sem acentos e caracteres especiais (underscore preservado).
        """
        nfkd_form = unicodedata.normalize('NFKD', input_str)
        only_ascii = nfkd_form.encode('ASCII', 'ignore').decode('utf-8')
        return re.sub(r'[^a-zA-Z0-9_ ]', '', only_ascii)

    def rename_duplicate_columns(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Renomeia colunas duplicadas de um DataFrame.
        Necessário para compatibilidade com o schema do BigQuery,
        que não aceita colunas com nomes duplicados.

        :param df: DataFrame original.
        :return: DataFrame com colunas renomeadas.
        """
        seen_columns = {}
        final_columns = {}

        for col in df.columns:
            clean_col = self.remove_accents_and_special_chars(col)
            if clean_col in seen_columns:
                seen_columns[clean_col].append(col)
            else:
                seen_columns[clean_col] = [col]

        for clean_col, col_list in seen_columns.items():
            if len(col_list) > 1:
                for i, original_col in enumerate(col_list):
                    clean = self.remove_accents_and_special_chars(original_col)
                    final_columns[original_col] = clean if i == 0 else f"{clean}_{i + 1}"
            else:
                final_columns[col_list[0]] = self.remove_accents_and_special_chars(
                    col_list[0]
                )

        df.rename(columns=final_columns, inplace=True)
        return df

    def prepare_dataframe_for_load(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Prepara o DataFrame para o load no BigQuery aplicando
        as correções técnicas necessárias em uma única chamada:
        - Remove acentos e caracteres especiais dos nomes das colunas
        - Renomeia colunas duplicadas

        :param df: DataFrame bruto extraído da API.
        :return: DataFrame pronto para o load.
        """
        df = self.rename_duplicate_columns(df)
        logger.info(
            f"DataFrame preparado para load: {len(df)} registros, {len(df.columns)} colunas."
        )
        return df

    # ------------------------------------------------------------------
    # UTILITÁRIOS DE DATA
    # ------------------------------------------------------------------

    def data_milissegundos(self, day: int) -> int:
        """
        Retorna a data do dia atual menos 'day' dias em milissegundos.
        Utilizado para filtros de data em chamadas de API (ex: Kommo).

        :param day: Número de dias a subtrair da data atual.
        :return: Timestamp em milissegundos.
        """
        data_anterior = dt.datetime.now() - dt.timedelta(days=day)
        timestamp_ms = int(data_anterior.timestamp() * 1000)
        logger.info(f"Timestamp calculado: {data_anterior.strftime('%Y-%m-%d')} → {timestamp_ms}ms")
        return timestamp_ms
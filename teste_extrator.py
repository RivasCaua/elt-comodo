"""
Script de teste KommoExecutor para um único cliente

Uso:
    python teste_extractor.py --conta nome_do_cliente

O script roda todas as extrações para a conta informada e exibe 
um resumo final com o status de cada tabela no bigquery 
"""

import argparse
import logging
import sys
 
from kommo_extractor import KommoExtractor
from kommo_crm.kommo_utils import KommoUtils


# ==================================
# Logging detalhado para o teste
# ==================================
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[
        logging.StreamHandler(sys.stdout),
    ],
)

logger = logging.getLogger(__name__)


# ===========================================
# Tabelas esperadas após a extração completa
# ===========================================
TABELAS_ESPERADAS = [
    ("kommo_raw", "contas"),
    ("kommo_raw", "tags"),
    ("kommo_raw", "usuarios"),
    ("kommo_raw", "pipelines"),
    ("kommo_raw", "statuses"),
    ("kommo_raw", "listas"),
    ("kommo_raw", "listas_elementos"),
    ("kommo_raw", "tarefas"),
    ("kommo_raw", "leads"),
    ("kommo_raw", "eventos"),
    ("kommo_raw", "dlq"),
]


def verificar_tabelas(extractor: KommoExtractor, conta: str):
    """
    Consulta o BigQuery e exibe um resumo detalhado de cada tabela:
    - total de registros
    - registros da conta testada 
    - Último _ingestion_timestamp
    """

    print("\n" + "=" * 60)
    print(f"Resumo da extração - conta {conta}")
    print("\n" "=" * 60)

    for dataset, tabela in TABELAS_ESPERADAS:
        # Coluna de filtro varia para a DLQ 
        col_conta = "account_subdomain"

        sql_total = f"""
            SELECT
                COUNT(*) AS total,
                COUNTIF({col_conta} = '{conta}') AS da_conta,
                MAX(_ingestion_timestamp) AS ultimo_ingestion
            FROM `{extractor.elt.bigquery_client.project}.{dataset}.{tabela}`
        """

        try:
            df = extractor.elt.query_bigquery(sql_total, tabela)
            total = df["total"].iloc[0]
            da_conta = df["da_conta"].iloc[0]
            ultimo = df["ultimo_ingestion"].iloc[0]
            status = "OK" if int(da_conta) > 0 else "NOK 0 registros para a conta"
            print(
                f"{status} {dataset}.{tabela:25s} "
                f"| total={total:>6} | conta={da_conta:>6} "
                f"| último ingestion: {str(ultimo)[:19]}"
            ) 

        except Exception as e:
            print(f"ERROR {dataset}.{tabela:25s} | tabela não encontrada ou erro: {e}")

    print("=" * 60 + "\n")


def main():
    parser = argparse.ArgumentParser(
        description="Testa o KommoExtractor para um único cliente"
    )
    parser.add_argument(
        "--conta",
        required=True,
        help="Nome da conta (subdomain) para testar ex: --conta comodoplanejados",
    )
    parser.add_argument(
        "--renovar-chaves",
        action="store_true",
        default=False,
        help="Renovar os tokens OAuth2 antes de extrair (padrão: False)."
    )
    args = parser.parse_args()

    conta_teste = args.conta

    # =============================================
    # Valida se a conta existe no clientes.json
    # =============================================
    clientes = KommoUtils.get_clientes()
    if conta_teste not in clientes:
        contas_disponiveis = sorted(clientes.keys())
        logger.error(
            f"Conta '{conta_teste}' não encontrada no clientes.json.\n"
            f"Contas disponíveis: {contas_disponiveis}"
        )
        sys.exit(1)

    logger.info(f"Iniciando teste para a conta: {conta_teste}")

    # =============================================
    # Renovação de chaves (opicional)
    # =============================================
    if args.renovar_chaves:
        logger.info("Renovando chaves OAuth2....")
        KommoUtils.renovar_chaves()

    # ===============================================
    # Instancia o extrator restrito à conta de teste
    # ===============================================
    extractor = KommoExtractor()
    extractor.lista_clientes = [conta_teste] # Considera somente uma conta 
    logger.info(f"Extrator configurado para: {extractor.lista_clientes}")

    # =============================
    # Executa todas as extrações
    # =============================
    etapas = [
        ("contas",               extractor.extrair_contas),
        ("tags",                 extractor.extrair_tags),
        ("usuarios",             extractor.extrair_usuarios),
        ("pipelines e statuses", extractor.extrair_pipelines_e_statuses),
        ("listas",               extractor.extrair_listas),
        ("tarefas",              extractor.extrair_tarefas),
        ("leads",                extractor.extrair_leads),
        ("eventos",              extractor.extrair_eventos),
    ]

    erros = []
    for nome, fn in etapas:
        print(f"\n{'─' * 50}")
        print(f"▶ Extraindo: {nome}")
        print(f"{'─' * 50}")

        try:
            fn()
        except Exception as e: 
            logger.error(f"Falha inexperada em [{nome}]: {e}")
            erros.append(nome)

    # ===========================
    # resumo final no BigQuery
    # ===========================
    verificar_tabelas(extractor, conta_teste)

    if erros:
        logger.warning(f"Etapas com falha inexperada: {erros}")
        sys.exit(1)
    else:
        logger.info("Teste concluído com sucesso, sem erros inesperados")


if __name__ == "__main__":
    main()

    
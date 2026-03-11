import pandas as pd
from comodo_utils.auxiliar_functions import ELTExecutor

executor = ELTExecutor()

df_teste = pd.DataFrame({
    'id': [1, 2, 3],
    'nome': ['Lead Teste 1', 'Lead Teste 2', 'Lead Teste 3'],
    'data_criacao': ['2026-03-04', '2026-03-04', '2026-03-04'],
    'status': ['novo', 'em_andamento', 'fechado']
})

executor.load_to_bigquery_raw(
    dataframe=df_teste,
    dataset_id='elt_teste',
    table_id='leads_teste',
)
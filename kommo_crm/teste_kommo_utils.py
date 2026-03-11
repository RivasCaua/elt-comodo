from kommo_crm.kommo_utils import KommoUtils

clientes = KommoUtils.get_clientes()
print(f"Total de contas: {len(clientes)}")
print(f"Contas: {list(clientes.keys())}")

# Pega a primeira conta para teste
primeira_conta = list(clientes.keys())[1]
dados_conta = clientes[primeira_conta]

headers = KommoUtils.get_headers(dados_conta)
print(f"\nConta testada: {primeira_conta}")
print(f"Headers gerados: {list(headers.keys())}")
print(f"Token type: {dados_conta.get('token_type')}")
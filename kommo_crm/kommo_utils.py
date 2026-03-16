import json
import logging

import requests
from comodo_utils.comodo_utils import ComodoUtils

logger = logging.getLogger(__name__)


class KommoUtils:
    """
    Classe com métodos utilitários para a extração de dados do Kommo.
    Suportando múltiplas contas (multi-tenant) via arquivo clientes.json no Cloud Storage.
    """

    BUCKET_NAME = 'comodo-data-lake'
    CHAVES_PATH = 'chaves/clientes.json'

    @staticmethod
    def _carregar_clientes(storage_client) -> dict:
        """
        Carrega o arquivo clientes.json do Cloud Storage.
        :return: Dicionário com os dados de todas as contas Kommo.
        """
        arquivo_chaves = storage_client.get_bucket(
            KommoUtils.BUCKET_NAME
        ).blob(KommoUtils.CHAVES_PATH)
        return json.loads(arquivo_chaves.download_as_string())

    @staticmethod
    def _salvar_clientes(storage_client, clientes: dict):
        """
        Salva o arquivo clientes.json atualizado no Cloud Storage.
        :param clientes: Dicionário atualizado com os dados de todas as contas.
        """
        arquivo_chaves = storage_client.get_bucket(
            KommoUtils.BUCKET_NAME
        ).blob(KommoUtils.CHAVES_PATH)
        arquivo_chaves.upload_from_string(json.dumps(clientes))

    @staticmethod
    def renovar_chave_individual(conta: str, dados: dict) -> dict:
        """
        Tenta renovar o access_token de uma única conta usando o refresh_token.
        Chamado sob demanda pelo extrator quando recebe 401 durante a extração
        — não é mais executado em massa antes de tudo.

        Se a renovação funcionar, atualiza o clientes.json e retorna os dados
        atualizados. Se falhar (refresh_token expirado), retorna os dados
        originais sem alteração — o access_token existente ainda pode ser válido
        (tokens Kommo podem ter validade de anos).

        :param conta: Nome da conta (subdomain).
        :param dados: Dicionário com os dados atuais da conta.
        :return: Dicionário com os dados da conta (tokens atualizados ou originais).
        """
        url = f"https://{dados['url']}.kommo.com/oauth2/access_token"
        payload = {
            'client_id': dados['client_id'],
            'client_secret': dados['secret'],
            'grant_type': 'refresh_token',
            'refresh_token': dados['refresh_token'],
            'redirect_uri': dados['redirect_uri'],
        }

        try:
            response = requests.post(url, json=payload, timeout=30)

            if response.status_code != 200:
                logger.warning(
                    f"[{conta}] Renovação não disponível "
                    f"(status {response.status_code}) — mantendo token existente."
                )
                return dados

            resp_json = response.json()

            if 'access_token' not in resp_json or 'refresh_token' not in resp_json:
                logger.warning(
                    f"[{conta}] Resposta incompleta na renovação — "
                    f"mantendo token existente."
                )
                return dados

            # Atualiza os tokens em memória
            dados['token_type'] = resp_json['token_type']
            dados['access_token'] = resp_json['access_token']
            dados['refresh_token'] = resp_json['refresh_token']

            # Persiste no clientes.json
            storage_client = ComodoUtils.gerar_storage_client()
            clientes = KommoUtils._carregar_clientes(storage_client)
            clientes[conta] = dados
            KommoUtils._salvar_clientes(storage_client, clientes)

            logger.info(f"[{conta}] Token renovado com sucesso.")
            return dados

        except Exception as e:
            logger.warning(
                f"[{conta}] Falha ao tentar renovar token: {e} "
                f"— mantendo token existente."
            )
            return dados

    @staticmethod
    def get_headers(dados_conta: dict) -> dict:
        """
        Retorna os headers de autenticação para chamadas de API do Kommo.
        :param dados_conta: Dicionário com os dados da conta (do clientes.json).
        :return: Dicionário com os headers HTTP.
        """
        return {
            'Authorization': f"Bearer {dados_conta['access_token']}",
            'Content-Type': 'application/json'
        }

    @staticmethod
    def get_clientes() -> dict:
        """
        Retorna o dicionário completo de clientes do Cloud Storage.
        Utilizado pelos scripts de extração para iterar sobre as contas.
        :return: Dicionário com os dados de todas as contas Kommo.
        """
        storage_client = ComodoUtils.gerar_storage_client()
        return KommoUtils._carregar_clientes(storage_client)
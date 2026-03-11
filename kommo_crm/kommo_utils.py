import json
import logging

import requests
from comodo_utils.comodo_utils import ComodoUtils

logger = logging.getLogger(__name__)

class KommoUtils:
    """
    Classe com métodos utilitários para a extração de dados do kommmo.
    Suportando múltiplas contas (multi-tenant) via arquivo clientes.json no Cloud Storage
    """

    BUCKET_NAME = 'comodo-data-lake'
    CHAVES_PATH = 'chaves/clientes.json'

    @staticmethod
    def _carregar_clientes(storage_client) -> dict:
        """
        Carrega o arquivo clientes.json do Cloud Storage
        :return: Dicionário com os dados de todas as contas Kommo
        """

        arquivo_chaves = storage_client.get_bucket(
            KommoUtils.BUCKET_NAME
        ).blob(KommoUtils.CHAVES_PATH)
        return json.loads(arquivo_chaves.download_as_string())
    
    
    @staticmethod
    def _salvar_clientes(storage_client, clientes : dict):
        """
        Salva o arquivo clientes.json atualizado no cloud storage
        :param clientes: Dicionário atualizado com os dados de todas as contas
        """

        arquivo_chaves = storage_client.get_bucket(
            KommoUtils.BUCKET_NAME
        ).blob(KommoUtils.CHAVES_PATH)
        arquivo_chaves.upload_from_string(json.dumps(clientes))

    
    @staticmethod
    def renovar_chaves():
        """
        Renova as chaves de acesso Oauth2 de todas as contas kommo.
        Itera sobre cada empresa no arquivo clientes.json e renova individualmente
        Contas com falha são logadas mas não interrompem a renovação dos demais 
        """

        storage_client = ComodoUtils.gerar_storage_client()
        clientes = KommoUtils._carregar_clientes(storage_client)

        sucesso = 0
        falhas = []

        for conta, dados in clientes.items():
            logger.info(f"Renovando chave: {conta}")

            url = f"https://{dados['url']}.kommo.com/oauth2/access_token"
            payload = {
                'client_id' : dados['client_id'],
                'client_secret' : dados['secret'],
                'grant_type' : 'refresh_token',
                'refresh_token' : dados['refresh_token'],
                'redirect_uri' : dados['redirect_uri']
            }

            try:
                response = requests.post(url, json=payload, timeout=30)

                if response.status_code != 200:
                    raise ValueError(
                        f"Status inesperado: {response.status_code} -> {response.text}"
                    )
                
                resp_json = response.json()

                # Valida se os campos esperados estão na resposta
                if 'access_token' not in resp_json or 'refresh_token' not in resp_json:
                    raise ValueError(f"Resposta incompleta da API: {resp_json}")
                
                # Atualiza os tokens da conta
                clientes[conta]['token_type'] = resp_json['token_type']
                clientes[conta]['access_token'] = resp_json['access_token']
                clientes[conta]['refresh_token'] = resp_json['refresh_token']

                sucesso += 1
                logger.info(f"Chave renovada com sucesso: {conta}")

            except Exception as e:
                falhas.append(conta)
                logger.error(f"Falha ao renovar chave [{conta}]: {e}")   
                # Continua para a próxima conta - não interfere o processo

        # Salva o arquivo atualizado apenas uma vez ao final
        KommoUtils._salvar_clientes(storage_client, clientes)

        logger.info(
            f"Renovação concluída: {sucesso} sucesso(s), {len(falhas)} falha(s)."
        )

        if falhas:
            logger.warning(f"Contas com falha na renovação: {falhas}")

    
    @staticmethod
    def get_headers(dados_conta: dict) -> dict:
        """
        Retorna os headers de autenticação para chamadas de API do Kommo.
        :param dados_conta: Dicionário com os dados da conta (do clientes.json)
        :return: Dicionário com os headers HTTP.
        """

        return{
            'Authorization' : f"Bearer {dados_conta['access_token']}",
            'Content-Type' : 'application/json'
        }
    

    @staticmethod
    def get_clientes() -> dict:
        """
        Retorna o dicionário completo de itens do Cloud Storage
        Utilizado pelos scripts de extração para iterar sobre as contas
        :return: Dicionário com os dados de todas as contas Kommo
        """

        storage_client = ComodoUtils.gerar_storage_client()
        return KommoUtils._carregar_clientes(storage_client)
    

if __name__ == '__main__':
    KommoUtils.renovar_chaves()
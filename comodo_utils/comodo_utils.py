import os
import logging
import smtplib
from email import encoders
from email import encoders
from email.mime.base import MIMEBase
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
import pandas as pd
from dotenv import load_dotenv
from google.cloud import storage

# Carrega o .env
load_dotenv()

# Configuração centralizada do logger
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class ComodoUtils:
    # Classe com métodos utilitários para o projeto.
    # Credenciais gerenciadas via variáveis de ambiente (.env)

    @staticmethod    
    def gerar_storage_client() -> storage.Client:
        # Gera um client autenticado do Google Cloud Storage
        # A credencial é lida da variável de ambiente GOOGLE_APPLICATION_CREDENTIALS.
        # :return: objeto storage. Client autenticado

        credencial = os.getenv('GOOGLE_APPLICATION_CREDENTIALS')

        if not credencial:
            raise EnvironmentError(
                "Variável GOOGLE_APPLICATION_CREDENTIALS não encontrada no .env"
            )
        
        os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = credencial
        logger.info("Storage Client criado com sucesso")
        return storage.Client()
    
    @staticmethod
    def enviar_email(anexo: pd.DataFrame = None, texto: str = "", assunto: str = "Aviso Cômodo"):
        # Envio de email com ou sem anexo em csv
        # Credenciais lidas via variáveis de ambiente 
        # :param anexo: DataFrame opicional a ser anexado como CSV
        # :param texto: corpo do email
        # :param assunto: assunto do email

        # Correção do anti-pattern de argumento mutável
        if anexo is None:
            anexo = pd.DataFrame()

        # Lê credenciais do .env
        smtp_server = os.getenv('SMTP_SERVER')
        smtp_port = int(os.getenv('SMTP_PORT', 465))
        smtp_username = os.getenv('SMTP_USERNAME')
        smtp_password = os.getenv('SMTP_PASSWORD')
        destinatarios = os.getenv('SMTP_DESTINATARIOS')

        if not all([smtp_server, smtp_port, smtp_username, smtp_password, destinatarios]):
            logger.error("Configurações de email incompletas no .env. Email não enviado")
            return
        
        # Monta o e-mail
        msg = MIMEMultipart()
        msg['Subject'] = assunto
        msg['From'] = smtp_username
        msg['To'] = destinatarios
        msg.attach(MIMEText(texto))

        # Anexa o DataFrame como csv se fornecido
        csv_path = 'error_log.csv'
        if not anexo.empty:
            try:
                anexo.to_csv(csv_path, index=False)
                with open(csv_path, 'rb') as f:
                    part = MIMEBase('application', 'octet-stream')
                    part.set_payload(f.read())
                    encoders.encode_base64(part)
                    part.add_header('Content-Disposition', f'attachment; filename="{csv_path}"')
                    msg.attach(part)

            except Exception as e:
                logger.error(f"Erro ao preparar anexo: {e}")
                return

        # Efetua o envio do e-mail
        server = None
        try:
            server = smtplib.SMTP_SSL(host=smtp_server, port=smtp_port)
            server.login(smtp_username, smtp_password)
            server.send_message(msg)
            logger.info(f"Email '{assunto}' enviado com sucesso para {destinatarios}.")

        except Exception as e:
            logger.error(f"Erro ao enviar e-mail '{assunto}' : {e}")
        finally:
            if server:
                server.quit()
            if not anexo.empty and os.path.exists(csv_path):
                os.remove(csv_path)
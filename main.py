from logging import getLogger
from os import environ

from dotenv import load_dotenv

from db import Connection

from migracao import (
    MigracaoTabelaPessoa,
    MigracaoTabelaPessoaFisica,
    MigracaoTabelaSuapFuncionario,
    MigracaoTabelaServidor,
    MigracaoTabelaAuthUser,
    MigracaoTabelaComumVinculo
)

env = '.env.local'
load_dotenv(env)

logger = getLogger(__name__)


if __name__ == '__main__':
    logger.info('Starting migration...')
    
    logger.info(f"Connecting to database with {env}")
    conn, url_conn = Connection(
        dbname=environ.get('DB_NAME'),
        user=environ.get('DB_USER'),
        password=environ.get('DB_PASSWORD'),
        host=environ.get('DB_HOST'),
        port=environ.get('DB_PORT')
    ).get_connection()
    
    logger.info(f"Databse {url_conn}")
    
    ## Tabela Pessoa
    pessoa = MigracaoTabelaPessoa(
        conn=conn,
        url_conn=url_conn
    )
    logger.info(f"Executing migration of {pessoa.table}...")
    
    # pessoa.execute()
    
    ## TODO: pegar o id de auth_user e inserir nas tabelas:
    # pessoa_fisica -> user_id (auth_user.id)
    # comum_vinculo -> user_id fk (auth_user.id)
       
    ## Tabela Auth User
    auth_user = MigracaoTabelaAuthUser(
        conn=conn,
        url_conn=url_conn
    )
    
    logger.info(f"Executing migration of {auth_user.table}...")
    # auth_user.execute()
    
    ## Tabela Pessoa Fisica
    pessoa_fisica = MigracaoTabelaPessoaFisica(
        conn=conn,
        url_conn=url_conn
    )
    
    logger.info(f"Executing migration of {pessoa_fisica.table}...")
    
    # pessoa_fisica.execute()
    
    ## Tabela Suap Funcionario
    suap_funcionario = MigracaoTabelaSuapFuncionario(
        conn=conn,
        url_conn=url_conn
    )
    logger.info(f"Executing migration of {suap_funcionario.table}...")
    
    # suap_funcionario.execute()
    
    servidor = MigracaoTabelaServidor(
        conn=conn,
        url_conn=url_conn
    )
    # servidor.execute()
    
    # TODO: review this tale
    # comum_vinculo = MigracaoTabelaComumVinculo(
    #     conn=conn,
    #     url_conn=url_conn
    # )
    # comum_vinculo.execute()
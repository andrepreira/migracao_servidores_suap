from random import randint
from datetime import datetime

from pandas import DataFrame, read_csv

from . import Migracao, MigracaoTabelaSuapFuncionario

class MigracaoTabelaServidor(Migracao):
    def __init__(self, conn, url_conn) -> None:
        super().__init__(conn, url_conn)
        self.table = 'servidor'
    
    def add_extra_fields(self, df: DataFrame) -> DataFrame:
        df['situacao_id'] = 1
        df['opera_raio_x'] = False
        df['alterado_em'] = datetime.now()
        return df
    
    def execute(self) -> None:
        df_raw = read_csv('./data/dados_suap_21112023.csv')
        suap_funcionario = MigracaoTabelaSuapFuncionario(
            conn=self.conn,
            url_conn=self.url_conn
        )
        df_raw = suap_funcionario.add_funcionario_id(df_raw)
        
        df_raw['email_siape'] = df_raw.apply(lambda raw: f"{raw['NOME'].split()[0].lower()}.{raw['NOME'].split()[-1].lower()}@gmail.com", axis=1)
        df_raw['email_academico'] = df_raw.apply(lambda raw: f"{raw['NOME'].split()[0].lower()}.{raw['NOME'].split()[-1].lower()}@gmail.com", axis=1)
        df_raw['email_google_classroom'] = df_raw.apply(lambda raw: f"{raw['NOME'].split()[0].lower()}.{raw['NOME'].split()[-1].lower()}@gmail.com", axis=1)
        df_raw['email_institucional'] = df_raw.apply(lambda raw: f"{raw['NOME'].split()[0].lower()}.{raw['NOME'].split()[-1].lower()}@uncisal.com.br", axis=1)
        df_raw['matricula'] = df_raw.apply(lambda x: randint(10000, 99999), axis=1)
        df_raw['matricula_sipe'] = df_raw.apply(lambda x: randint(10000, 99999), axis=1)
        df_raw['matricula_crh'] = df_raw.apply(lambda x: randint(10000, 99999), axis=1)
        df_raw['matricula_anterior'] = df_raw.apply(lambda x: randint(10000, 99999), axis=1)
  
        dict_non_null = {
            'funcionario_ptr_id': 'funcionario_ptr_id',
            'pessoa_ptr_id': 'pessoa_fisica_id',
            'email_siape': 'email_siape',
            'email_academico': 'email_academico',
            'email_google_classroom': 'email_google_classroom',
            'email_institucional': 'email_institucional',
            'matricula': 'matricula',
            'matricula_sipe': 'matricula_sipe',
            'matricula_crh': 'matricula_crh',
            'matricula_anterior': 'matricula_anterior',
        }
        
        dict_null = {}
        df_mapped = self.treatment_data_table(
            df_raw=df_raw,
            dict_non_null=dict_non_null,
            dict_null=dict_null
        )
        df_mapped = self.add_extra_fields(df_mapped)
        df_mapped.to_sql(self.table, self.url_conn, if_exists="append", index=False)
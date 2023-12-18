from datetime import datetime

from dotenv import load_dotenv
from pandas import DataFrame, read_csv

from migracao import Migracao

load_dotenv('.env.local')

class MigracaoTabelaAuthUser(Migracao):
    def __init__(self, conn, url_conn):
        super().__init__(conn, url_conn)
        self.table = 'auth_user'
    
    def add_extra_fields(self, df: DataFrame) -> DataFrame:
        df['is_superuser'] = False
        df['is_active'] = True
        df['is_staff'] = False
        df['eh_servidor'] = False
        df['eh_aluno'] = False
        df['eh_prestador'] = False
        df['eh_docente'] = True
        df['eh_tecnico_administrativo'] = False
        df['login_attempts'] = 3
        df['date_joined'] = datetime.now()
        df['uuid'] = df.apply(lambda row: self.generate_uuid(), axis=1)
        df['forcar_troca_senha'] = False
        
        return df
    
    def execute(self):
        df_raw = read_csv('./data/dados_suap_21112023.csv')
        df_raw['first_name'] = df_raw.apply(lambda row: row['NOME'].split()[0].lower(), axis=1)
        df_raw['last_name'] = df_raw.apply(lambda row: row['NOME'].split()[-1].lower(), axis=1)
        df_raw['username'] = df_raw['first_name'] +'.'+ df_raw['last_name']
        df_raw['password'] = '33237525f92eca85ff1287f6022b02b9' #suap@2024
        df_raw['email'] = df_raw.apply(lambda raw: f"{raw['NOME'].split()[0].lower()}.{raw['NOME'].split()[-1].lower()}@uncisal.com.br", axis=1)

        dict_non_null = {
            'MATRICULA': 'username',
            'password': 'password',
            'first_name': 'first_name',
            'last_name': 'last_name',
            'email': 'email',
        }
        
        dict_null = {}
        
        df_mapped = self.treatment_data_table(
            df_raw=df_raw,
            dict_non_null=dict_non_null,
            dict_null=dict_null
        )
        
        df_mapped = self.add_extra_fields(df_mapped)
        df_mapped.to_sql(self.table, self.url_conn, if_exists="append", index=False)
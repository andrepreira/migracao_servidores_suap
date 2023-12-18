
from pandas import read_csv, DataFrame

from . import Migracao

class MigracaoTabelaPessoa(Migracao):
    
    def __init__(self, conn, url_conn) -> None:
        super().__init__(conn, url_conn)
        self.table = 'pessoa'
        
    def add_pessoa_id(self, df_raw: DataFrame) -> DataFrame: 
            df_raw['pessoa_ptr_id'] = df_raw['NOME'].apply(lambda x: self.query_id_by_name(column_name='NOME', where_value=x))
            return df_raw
    
    def add_extra_fields(self, df):
        df['excluido'] = False
        df['natureza_juridica'] = '101'


        df['uuid'] = df.apply(lambda row: self.generate_uuid(), axis=1)

        return df
                
    def execute(self):    

        df_raw = read_csv('./data/dados_suap_21112023.csv')
        
        df_raw['email'] = df_raw.apply(lambda raw: f"{raw['NOME'].split()[0].lower()}.{raw['NOME'].split()[-1].lower()}@uncisal.com.br", axis=1)
        df_raw['email_secundario'] = df_raw.apply(lambda raw: f"{raw['NOME'].split()[0].lower()}.{raw['NOME'].split()[-1].lower()}@gmail.com", axis=1)
        
        de_para_dict_non_null = {
            'NOME': 'nome',
            'email': 'email',
            'email_secundario': 'email_secundario',
        }
        
        null_fields_and_default_values = {
            'nome_social': 'nao-informado',  # Example default value
            'nome_registro': 'nao-informado',  # Example default value
            'nome_usual': 'nao-informado',
            'website': 'nao-informado',  # Example default value
            'sistema_origem': 'nao-informado',  # Example default value
            'search_fields_optimized': 'nao-informado',  # Example default value
        }
        
        df_mapped = self.treatment_data_table(
            df_raw=df_raw,
            dict_non_null=de_para_dict_non_null,
            dict_null=null_fields_and_default_values
        )
        
        df_mapped = self.add_extra_fields(df_mapped)
        
        df_mapped.to_sql(self.table, self.url_conn, if_exists="append", index=False)

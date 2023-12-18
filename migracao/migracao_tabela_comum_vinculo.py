from pandas import read_csv

from migracao import Migracao, MigracaoTabelaPessoa, MigracaoTabelaAuthUser

class MigracaoTabelaComumVinculo(Migracao):
    
    def __init__(self, conn, url_conn):
        super().__init__(conn, url_conn)
        self.table = 'comum_vinculo'
        
    def add_extra_fields(self, df):
        return df
    
    def execute(self) -> None:
        self._logger.error(f"Executing migration of {self.table}...")
        
        df_raw = read_csv('./data/dados_suap_21112023.csv')
        pessoa = MigracaoTabelaPessoa(
            conn=self.conn,
            url_conn=self.url_conn
        )
        df_raw = pessoa.add_pessoa_id(df_raw)
        de_para_dict_non_null = {
            'pessoa_ptr_id': 'pessoa_id'
        }
        
        # TODO: review this fields
        # tipo_relacionamento_id int4 NOT NULL,
        # id_relacionamento int4 NOT NULL,
        # "search" text NOT NULL,
        
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
        
        exit()
        df_mapped.to_sql(self.table, self.url_conn, if_exists="append", index=False)



        
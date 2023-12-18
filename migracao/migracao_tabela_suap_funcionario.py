
from pandas import read_csv, DataFrame

from . import Migracao,MigracaoTabelaPessoa

class MigracaoTabelaSuapFuncionario(Migracao):
    
    def __init__(self, conn, url_conn):
        super().__init__(conn, url_conn)
        self.table = 'suap_funcionario'
    
    def add_funcionario_id(self, df_raw: DataFrame) -> DataFrame:
        pessoa = MigracaoTabelaPessoa(
            conn=self.conn,
            url_conn=self.url_conn
        )
        df_pessoa = pessoa.add_pessoa_id(df_raw) 
        df_raw['funcionario_ptr_id'] = df_pessoa['pessoa_ptr_id'].apply(
            lambda x: self.query_field_by_value(
                field='pessoafisica_ptr_id',
                column_name='pessoafisica_ptr_id', where_value=x
            )
        )
        return df_raw

    def add_extra_fields(self, df):
        return df
    
    def execute(self) -> None:
        df_raw = read_csv('./data/dados_suap_21112023.csv')
        pessoa = MigracaoTabelaPessoa(
            conn=self.conn,
            url_conn=self.url_conn
        )
        df_raw = pessoa.add_pessoa_id(df_raw)
        dict_non_null = {
            'pessoa_ptr_id': 'pessoafisica_ptr_id',
        }
        
        dict_null = {
            'setor_id': 1,
            # 'setor_funcao_id': 0,
            # 'setor_lotacao_id': 0
        }
        df_mapped = self.treatment_data_table(
            df_raw=df_raw,
            dict_non_null=dict_non_null,
            dict_null=dict_null
        )
        df_mapped.to_sql(self.table, self.url_conn, if_exists="append", index=False)
from dotenv import load_dotenv

from pandas import DataFrame, read_csv

from . import Migracao, MigracaoTabelaPessoa

load_dotenv('.env.local')

class MigracaoTabelaPessoaFisica(Migracao):
    
    def __init__(self, conn, url_conn):
        super().__init__(conn, url_conn)
        self.table = 'pessoa_fisica'
    
    def add_extra_fields(self, df: DataFrame) -> DataFrame:
        df['eh_servidor'] = True
        df['eh_aluno'] = False
        df['eh_prestador'] = False
        df['tem_digital_fraca'] = False
        df['template_importado_terminal'] = False
        df['lattes'] = 'http://lattes.cnpq.br/7166857281161500'
        return df
                
    def execute(self):
        pessoa = MigracaoTabelaPessoa(
            conn=self.conn,
            url_conn=self.url_conn
        )
        df_raw = read_csv('./data/dados_suap_21112023.csv')
        df_raw = df_raw[['NOME', 'CPF', 'NOME DA MAE', 'NOME DO PAI', 'DATA DE NASCIMENTO', 'SEXO BIOLÓGICO']]        
        df_raw = pessoa.add_pessoa_id(df_raw)
        df_raw['CPF'] = df_raw['CPF'].apply(lambda x: x[:2]+'.'+x[2:5]+'.'+x[5:])
        df_raw['SEXO BIOLÓGICO'] = df_raw['SEXO BIOLÓGICO'].apply(lambda x: x[:1])
        
        # print(df_raw['NOME'].apply(lambda x: print(x)))

        # passaporte
        dict_null = {
            'passaporte': 'nao-informado',
            'senha_ponto': 'nao-informado',
        }
        
        dict_non_null = {
            'pessoa_ptr_id': 'pessoa_ptr_id',
            'CPF': 'cpf',
            'NOME DA MAE': 'nome_mae',
            'NOME DO PAI': 'nome_pai',
            'SEXO BIOLÓGICO': 'sexo',
        }

        df_mapped = self.treatment_data_table(
            df_raw=df_raw,
            dict_non_null=dict_non_null,
            dict_null=dict_null
        )

        df_mapped = self.add_extra_fields(df_mapped)
        print(df_mapped)
        df_mapped.to_sql(self.table, self.url_conn, if_exists="append", index=False)
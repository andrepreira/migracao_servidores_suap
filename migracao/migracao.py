import logging
from typing import Dict,List
from os import environ
from uuid import uuid4
from abc import ABC, abstractmethod

from pandas import read_sql_query, DataFrame
from sqlalchemy import create_engine,MetaData,Table

class Migracao(ABC):
    
    def __init__(self, conn, url_conn) -> None:
        self.conn = conn
        self.url_conn = url_conn
        self._loglevel = environ.get('LOGLEVEL', 'DEBUG').upper()
        self._logger = logging.getLogger(__name__)
        self._logger.setLevel(self._loglevel)

    # Function to generate a UUID
    def generate_uuid(self):
        return uuid4()
    
    def query_field_by_value(self, field: str, column_name: str, where_value: int) -> DataFrame:
        
        engine = create_engine(self.url_conn)
        
        formatted_where_value = where_value
    
        # Query the database
        query = f"SELECT {field} FROM {self.table} WHERE {column_name}={formatted_where_value}"

        # Query the database
        df = read_sql_query(query, engine)

        # Close the connection
        engine.dispose()

        # Return the DataFrame
        return df[f"{field}"][0]
    def query_id_by_name(self, column_name: str, where_value: str) -> int:
        """
        Connects to a database and executes a query.

        :param column_name: str name of colunm
        :param where_value: str 
        :return: int id
        """

        # Create a database connection
        engine = create_engine(self.url_conn)
        
        formatted_where_value = f"'{where_value}'"

        # Query the database
        query = f"SELECT id FROM {self.table} WHERE {column_name}={formatted_where_value}"

        # Query the database
        df = read_sql_query(query, engine)

        # Close the connection
        engine.dispose()

        # Return the DataFrame
        return df['id'][0]
    
    def get_all_ids(self) -> List[int]:
        """
        Connects to a database and retrieves all IDs from a specified table.

        :param table_name: Name of the table to query.
        :return: A Pandas Series containing all IDs.
        """

        # Create a database connection
        engine = create_engine(self.url_conn)

        # Query the database
        query = f"SELECT id FROM {self.table}"
                
        df = read_sql_query(query, engine)

        # Close the connection
        engine.dispose()

        # Return the IDs
        return df['id'].tolist()
    
    def get_table_info(self) -> List[str]:
        """
        Connects to a database and retrieves column information for a specified table.

        :param db_connection_string: A database connection string.
        :param table_name: Name of the table to reflect.
        :return: A list of column names.
        """

        engine = create_engine(self.url_conn)
        metadata = MetaData()
        table = Table(self.table, metadata, autoload_with=engine)

        # Get column names
        column_names = [column.name for column in table.columns]

        return column_names
    
    @abstractmethod
    def add_extra_fields(self, df: DataFrame) -> DataFrame:
        return df  
      
    def treatment_data_table(self, df_raw: DataFrame, dict_non_null: Dict[str, str], dict_null: Dict[str, str] ) -> DataFrame:
        """
            This method recieves a DataFrame and two dict with the columns that will be mapped.
            The first dict is the columns that will be mapped to a non null value and second dict
            is the columns that will be mapped to a null value and the defaut values.
            params: df_raw -> DataFrame
            params: dict_non_null -> Dict[str]
            params: dict_null -> Dict[str]
            return: df_mapped -> DataFrame
        """
        df_mapped = df_raw[list(dict_non_null.keys())].copy()
        df_mapped.columns = [dict_non_null[col] for col in df_mapped.columns]


        df_mapped = df_mapped.assign(**dict_null)
        
        return df_mapped
 
    @abstractmethod
    def execute(self) -> None:    
        pass
        
from  typing import Tuple

from psycopg2 import connect
from psycopg2.extensions import connection

class Connection:
    # Establish a connection to your PostgreSQL database
    def __init__(self, dbname, user, password, host, port):
        self.dbname=dbname
        self.user=user
        self.password=password
        self.host=host
        self.port=port

    def get_connection(self) -> Tuple[connection, str]:
        """
            Returns a connection and engine to a PostgreSQL database               
        """
        try:
            db_connection = connect(
                dbname=self.dbname,
                user=self.user,
                password=self.password,
                host=self.host,
                port=self.port
            )

            return db_connection, f"postgresql://{self.user}:{self.password}@{self.host}:{self.port}/{self.dbname}"
        except Exception as e:
            print(e)

 
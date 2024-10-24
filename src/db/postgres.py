import psycopg2
import pandas as pd


class PostgresConnector:
    def __init__(self, host, port, username, password, database):
        self.host = host
        self.port = port
        self.username = username
        self.password = password
        self.database = database
        self.conn = None
        self.cursor = None

    def connect(self):
        self.conn = psycopg2.connect(user=self.username, password=self.password, host=self.host,
                                     database=self.database, port=self.port)
        self.cursor = self.conn.cursor()

    def disconnect(self):
        if self.cursor is not None:
            self.cursor.close()

        if self.conn is not None:
            self.conn.close()

    def run_sql(self, query: str) -> pd.DataFrame:
        self.cursor.execute(query)
        result = self.cursor.fetchall()
        columns = [desc[0] for desc in self.cursor.description]
        return pd.DataFrame(result, columns=columns)

import psycopg2
import pandas as pd

from typing import List
from src.config.config import global_cfg


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

    def get_databases(self) -> List[str]:
        df = self.run_sql("SELECT datname FROM pg_database WHERE datistemplate = false;")
        return df["datname"].tolist()

    def get_information_tables(self, database: str) -> List[str]:
        df = self.run_sql(f"SELECT table_name FROM information_schema.tables WHERE table_schema = 'public' AND table_catalog = '{database}';")
        return df["table_name"].tolist()

    def get_schema_from_database(self) -> pd.DataFrame:
        data = []
        databases = self.get_databases()

        for database in databases:
            if database != global_cfg.db:
                continue

            df_tables = self.get_information_tables(database=database)

            for table in df_tables:
                print(f"Trying get schema and columns for table {database}.{table}")

                df_columns = self.run_sql(
                    f"""
                    SELECT 
                        c.column_name, 
                        c.data_type, 
                        pgd.description AS column_comment 
                    FROM 
                        information_schema.columns c 
                    LEFT JOIN 
                        pg_catalog.pg_statio_all_tables st 
                        ON c.table_schema = st.schemaname 
                        AND c.table_name = st.relname 
                    LEFT JOIN 
                        pg_catalog.pg_description pgd 
                        ON pgd.objoid = st.relid 
                        AND pgd.objsubid = c.ordinal_position 
                    WHERE 
                        c.table_catalog = '{database}' 
                        AND c.table_name = '{table}' 
                        AND c.table_schema = 'public';
                    """
                )

                for index, row in df_columns.iterrows():
                    data.append({
                        "database": database,
                        "table_schema": "public",
                        "table_name": table,
                        "column_name": row["column_name"],
                        "data_type": row["data_type"],
                        "comment": row["column_comment"] if "column_comment" in row else None
                    })

        return pd.DataFrame(data)

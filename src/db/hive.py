from typing import List
import pandas as pd
from pyhive import hive

from src.config.config import global_cfg


class HiveConnector:
    def __init__(self, host, port, username, password, database):
        self.host = host
        self.port = port
        self.username = username
        self.password = password
        self.database = database
        self.conn = None
        self.cursor = None

    def connect(self):
        self.conn = hive.connect(host=self.host, port=self.port, username=self.username,
                                 password=self.password, database=self.database, auth='CUSTOM')
        self.cursor = self.conn.cursor()

    def disconnect(self):
        if self.cursor is not None:
            self.cursor.close()

    def run_sql(self, query: str) -> pd.DataFrame:
        self.cursor.execute(query)
        result = self.cursor.fetchall()
        columns = [desc[0] for desc in self.cursor.description]
        return pd.DataFrame(result, columns=columns)

    def get_databases(self) -> List[str]:
        df = self.run_sql("SHOW DATABASES")
        return df["namespace"].tolist()

    def get_information_tables(self, database: str) -> List[str]:
        df = self.run_sql(f"SHOW TABLES IN {database}")
        return df["tableName"].tolist()

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
                    f"DESCRIBE {database}.{table}"
                )

                for index, row in df_columns.iterrows():
                    data.append({
                        "database": database,
                        "table_schema": database,
                        "table_name": table,
                        "column_name": row["col_name"],
                        "data_type": row["data_type"],
                        "comment": row["comment"]
                    })

            return pd.DataFrame(data)

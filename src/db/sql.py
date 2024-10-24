import pandas as pd
from src.config.config import global_cfg
from src.db.hive import HiveConnector
from src.db.postgres import PostgresConnector


def run_sql(sql: str) -> pd.DataFrame:
    if global_cfg.dbtype == 'postgres':

        print("This is a Postgres SQL connection. run sql with postgres connection, sql message is:", sql)

        pg_connector = PostgresConnector(host=global_cfg.host, port=global_cfg.port, username=global_cfg.user,
                                         password=global_cfg.password, database=global_cfg.db)

        pg_connector.connect()
        df = pg_connector.run_sql(sql)
        # 关闭连接
        pg_connector.disconnect()

        return df
    elif global_cfg.dbtype == 'hive':

        cleaned_sql_query = sql.rstrip(';')
        print("This is a hive SQL connection. run sql with hive connection, sql message is:", cleaned_sql_query)

        hive_connector = HiveConnector(host=global_cfg.host, port=global_cfg.port, username=global_cfg.user,
                                       password=global_cfg.password, database=global_cfg.db)

        hive_connector.connect()
        df = hive_connector.run_sql(sql)
        # 关闭连接
        hive_connector.disconnect()

        return df

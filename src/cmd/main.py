import os
import sys
from openai import OpenAI

from src.vanna.chromadb import ChromaDB_VectorStore
from src.vanna.flask import VannaFlaskApp
from src.vanna.openai import OpenAI_Chat
from src.util.path import check_directory_exists

from src.config.config import global_cfg
from src.db.sql import run_sql
from src.db.hive import HiveConnector


class MyVanna(ChromaDB_VectorStore, OpenAI_Chat):
    def __init__(self, chat_client=None, chat_config=None):
        if chat_config is None:
            chat_config = {'path': ''}
        if chat_config is not None:
            ChromaDB_VectorStore.__init__(self, config=chat_config)
        if chat_client is not None:
            OpenAI_Chat.__init__(self, config=chat_config, client=chat_client)


def main():
    ini_file_path = './src/cmd/config.ini'

    if os.path.exists(ini_file_path):
        print(f"{ini_file_path} exist, get parameter from this file")
        global_cfg.parse_ini_file(ini_file_path)
    else:
        print(f"{ini_file_path} not exist, get parameter from command line")
        global_cfg.parse_command_line_args()

    if (
            global_cfg.dbtype is None or global_cfg.host is None or
            global_cfg.port is None or global_cfg.temperature is None
            or global_cfg.baseurl is None or global_cfg.apikey is None
            or global_cfg.model is None):
        print("Error: Missing required arguments!! dbtype, host, port, "
              "temperature, baseurl, apikey and model must be set"
             )
        sys.exit(1)

    print(global_cfg.__repr__())

    cli = OpenAI(base_url=global_cfg.baseurl, api_key=global_cfg.apikey)
    if global_cfg.dbtype == 'postgres':
        sql_train = ('You are an expert in generating SQL queries and '
                     'extracting dimensions, metrics, joins,'
                     'and filters for PostgresSQL databases. Your task is to '
                     'convert natural language business'
                     'intelligence analysis requirements into PostgresSQL '
                     'queries and extract the necessary information')

        if check_directory_exists('./src/pg-chroma'):
            print('directory pg-chroma already exist')
        else:
            print('directory pg-chroma has not exist, so create it')
            os.makedirs("./src/pg-chroma")

        ai_cfg = {'model': global_cfg.model,
                  'path': './src/pg-chroma',
                  'temperature': float(global_cfg.temperature)
                  }

        vn = MyVanna(chat_config=ai_cfg, chat_client=cli,)
        vn.train(documentation=sql_train)
        vn.run_sql = run_sql
        vn.run_sql_is_set = True
    elif global_cfg.dbtype == 'hive':
        sql_train = ('You are an expert in generating SQL queries and '
                     'extracting dimensions, metrics, joins,'
                     'and filters for Hive databases using Spark SQL. Your '
                     'task is to convert natural language'
                     'business intelligence analysis requirements into Spark '
                     'SQL queries and extract the necessary'
                     'information')

        if check_directory_exists('./src/hive-chroma'):
            print('directory hive-chroma already exist')
        else:
            print('directory pg-chroma has not exist, so create it')
            os.makedirs("./src/hive-chroma")

        ai_cfg = {'model': global_cfg.model,
                  'path': './src/hive-chroma',
                  'temperature': float(global_cfg.temperature)
                  }

        vn = MyVanna(chat_config=ai_cfg, chat_client=cli)
        hive_connector = HiveConnector(host=global_cfg.host,
                                       port=global_cfg.port,
                                       username=global_cfg.user,
                                       password=global_cfg.password,
                                       database=global_cfg.db)
        hive_connector.connect()
        df = hive_connector.get_schema_from_database()
        hive_connector.disconnect()

        print(df)

        plan = vn.get_training_plan_generic(df)
        vn.train(plan=plan)
        vn.train(documentation=sql_train)
        vn.run_sql = run_sql
        vn.run_sql_is_set = True
    else:
        print("unsupported database type:", global_cfg.dbtype)
        sys.exit(2)

    VannaFlaskApp(vn, allow_llm_to_see_data=True).run()


if __name__ == '__main__':
    main()

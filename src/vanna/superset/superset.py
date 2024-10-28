import os
import requests
from dataclasses import dataclass


@dataclass
class SuperSetConfig:
    superset_url: str
    username: str
    password: str


# if __name__ == "__main__":
#    config = SuperSetConfig(
#        superset_url="http://172.21.84.134:9000",
#        username="admin",
#        password="admin"
#    )
#    superset_api = SuperSet_API(config)
#    access_token = superset_api.login()
#    if access_token:
#        print(f"Access Token: {access_token}")
#    else:
#        print("Failed to get access token.")
#
#    db_url = "postgresql://xxxx:xxxx@192.168.1.1:5432/xxxx"
#    db_id = superset_api.create_database(db_url, access_token)

#    if db_id:
#        print(f"Database created successfully with ID: {db_id}")
#    else:
#        print("Failed to create database.")
class SuperSet_API:
    def __init__(self, cfg: SuperSetConfig):
        self.cfg = cfg

    # 登录接口
    def login(self):
        login_url = f"{self.cfg.superset_url}/api/v1/security/login"

        payload = {
            "password": self.cfg.password,
            "provider": "db",
            "refresh": True,
            "username": self.cfg.username
        }

        response = requests.post(login_url, json=payload)
        if response.status_code == 200:
            response_data = response.json()
            token = response_data.get("access_token")
            return token
        else:
            print(f"Login failed with status code {response.status_code}")
            print(response.text)
            return None

    # 获取database
    def get_database_with_name(self, database_url, token):
        engine = database_url.split(':')[0]
        database_name = engine + "-" + "Vanna"

        get_databases_url = f"{self.cfg.superset_url}/api/v1/database/?q=(filters:!((col:database_name,opr:ct,value:{database_name})),order_column:changed_on_delta_humanized,order_direction:desc,page:0,page_size:25)"
        headers = {
          "Authorization": f"Bearer {token}"
        }

        response = requests.get(get_databases_url, headers=headers)
        if response.status_code == 200:
            response_data = response.json()
            count = response_data.get("count")
            if count == 0:
                return None
            else:
                return response_data.get("ids")[0]
        else:
            print(
                f"Get databases failed with status code "
                f"{response.status_code}")
            print(response.text)
            return None

    # 从database_url中创建database
    def create_database(self, database_url, token):
        engine = database_url.split(':')[0]
        create_database_url = f"{self.cfg.superset_url}/api/v1/database/"

        payload = {
          "engine": engine,
          "configuration_method": "sqlalchemy_form",
          "database_name": engine + "-" + "Vanna",
          "sqlalchemy_uri": database_url
        }

        headers = {
          "Authorization": f"Bearer {token}"
        }

        response = requests.post(create_database_url,
                                 json=payload, headers=headers)

        if response.status_code == 200:
            response_data = response.json()
            return response_data.get("id")
        else:
            print(
                f"Create database failed with status code "
                f"{response.status_code}")
            print(response.text)
            return None

    # 从table_name, database_id获取dataset
    def get_dataset_with_name(self, table_name, database_id, token):
        get_databases_url = f"{self.cfg.superset_url}api/v1/dataset/?q=(filters:!((col:table_name,opr:ct,value:{table_name}),(col:database,opr:rel_o_m,value:{database_id})),order_column:changed_on_delta_humanized,order_direction:desc,page:0,page_size:25)"

        headers = {
          "Authorization": f"Bearer {token}"
        }

        response = requests.get(get_databases_url, headers=headers)
        if response.status_code == 200:
            response_data = response.json()
            count = response_data.get("count")
            if count == 0:
                return None
            else:
                return response_data.get("ids")[0]
        else:
            print(
                f"Get datasets failed with status code "
                f"{response.status_code}")
            print(response.text)
            return None

    # 创建dataset
    def create_dataset_with_table(self, database_id, schema, table_name, token):
        create_dataset_url = f"{self.cfg.superset_url}/api/v1/dataset/"

        payload = {
          "database": database_id,
          "schema": schema,
          "table_name": table_name
        }

        headers = {
          "Authorization": f"Bearer {token}"
        }

        response = requests.post(create_dataset_url,
                                 json=payload, headers=headers)

        if response.status_code == 200:
            response_data = response.json()
            return response_data.get("id")
        else:
            print(
                f"Create dataset failed with status code "
                f"{response.status_code}")
            print(response.text)
            return None

import requests
from dataclasses import dataclass


@dataclass
class SuperSetConfig:
    superset_url: str
    username: str
    password: str


class SuperSet_API:
    def __init__(self, cfg=None, token=None):
        self.cfg = cfg
        self.token = token

    def set_cfg(self, cfg: SuperSetConfig):
        self.cfg = cfg

    def set_token(self, token: str):
        self.token = token

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
            self.set_token(token)
            return token, None
        else:
            return None, f"Login failed with status code {response.status_code}, error message is {response.text}"

    # 获取database
    def get_database_with_name(self, database_url):
        engine = database_url.split(':')[0]
        database_name = engine + "-" + "Vanna"

        get_databases_url = f"{self.cfg.superset_url}/api/v1/database/?q=(filters:!((col:database_name,opr:ct,value:{database_name})),order_column:changed_on_delta_humanized,order_direction:desc,page:0,page_size:25)"
        headers = {
          "Authorization": f"Bearer {self.token}"
        }

        response = requests.get(get_databases_url, headers=headers)
        if response.status_code == 200:
            response_data = response.json()
            count = response_data.get("count")
            if count == 0:
                return None, None
            else:
                return response_data.get("ids")[0], None
        else:
            return None, f"Get databases failed with status code {response.status_code}, error message is {response.text}"

    # 从database_url中创建database
    def create_database(self, database_url):
        engine = database_url.split(':')[0]
        create_database_url = f"{self.cfg.superset_url}/api/v1/database/"

        payload = {
          "engine": engine,
          "configuration_method": "sqlalchemy_form",
          "database_name": engine + "-" + "Vanna",
          "sqlalchemy_uri": database_url
        }

        headers = {
          "Authorization": f"Bearer {self.token}"
        }

        response = requests.post(create_database_url,
                                 json=payload, headers=headers)

        if response.status_code == 201:
            response_data = response.json()
            return response_data.get("id"), None
        else:
            return None, f"Create database failed with status code {response.status_code}, error message is {response.text}"

    # 从table_name, database_id获取dataset
    def get_dataset_with_name(self, table_name, schema, database_id):
        get_databases_url = f"{self.cfg.superset_url}/api/v1/dataset/?q=(filters:!((col:table_name,opr:ct,value:{table_name}),(col:database,opr:rel_o_m,value:{database_id}),(col:schema,opr:eq,value:{schema})),order_column:changed_on_delta_humanized,order_direction:desc,page:0,page_size:25)"

        headers = {
          "Authorization": f"Bearer {self.token}"
        }

        response = requests.get(get_databases_url, headers=headers)
        if response.status_code == 200:
            response_data = response.json()
            count = response_data.get("count")
            if count == 0:
                return None, None
            else:
                return response_data.get("ids")[0], None
        else:
            return None,  f"Get datasets failed with status code {response.status_code}, error message is {response.text}"

    # 创建dataset
    def create_dataset_with_table(self, database_id, schema, table_name):
        create_dataset_url = f"{self.cfg.superset_url}/api/v1/dataset/"

        payload = {
          "database": database_id,
          "schema": schema,
          "table_name": table_name
        }

        headers = {
          "Authorization": f"Bearer {self.token}"
        }

        response = requests.post(create_dataset_url,
                                 json=payload, headers=headers)

        if response.status_code == 201:
            response_data = response.json()
            return response_data.get("id"), None
        else:
            return None, f"Create dataset failed with status code {response.status_code}, error message is {response.text}"

    def get_sql_and_data_from_superset(self, payload):
        get_sql_and_data_from_superset_url = f"{self.cfg.superset_url}/api/v1/chart/data"

        headers = {
          "Authorization": f"Bearer {self.token}"
        }

        response = requests.post(get_sql_and_data_from_superset_url,
                                 json=payload, headers=headers)

        if response.status_code == 201:
            response_data = response.json()
            return response_data.get("query"), None
        else:
            return None, f"Get sql from superset failed with status code {response.status_code}, error message is {response.text}"

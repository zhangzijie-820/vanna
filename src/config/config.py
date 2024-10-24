import argparse
import configparser


class GlobalConfig:
    def __init__(self, dbtype=None, host=None, port=None, user=None, password=None, db=None,
                 temperature=None, apikey=None, baseurl=None, model=None):
        self.dbtype = dbtype
        self.host = host
        self.port = port
        self.user = user
        self.password = password
        self.db = db
        self.apikey = apikey
        self.baseurl = baseurl
        self.model = model
        self.temperature = temperature

    def __repr__(self):
        # 为了方便打印，我们可以定义__repr__方法
        return (f"GlobalConfig(dbtype={self.dbtype}, host={self.host}, port={self.port}, user={self.user}, "
                f"password={self.password}, db={self.db},temperature={self.temperature}, apikey={self.apikey}, "
                f"baseurl={self.baseurl}, model={self.model})")

    def parse_ini_file(self, ini_file_path):
        """从INI文件解析配置参数"""
        config = configparser.ConfigParser()
        config.read(ini_file_path)

        # 如果没有读取到任何section，则认为INI文件不存在或为空
        if not config.sections():
            return None

        settings = config['settings']
        deepseek = config['deepSeekSet']

        self.dbtype = settings.get('dbtype', fallback=None)
        self.host = settings.get('host', fallback=None)
        self.port = settings.getint('port', fallback=None)
        self.user = settings.get('user', fallback=None)
        self.password = settings.get('password', fallback=None)
        self.db = settings.get('db', fallback=None)
        self.baseurl = deepseek.get('baseurl', fallback=None)
        self.apikey = deepseek.get('apikey', fallback=None)
        self.model = deepseek.get('model', fallback=None)
        self.temperature = deepseek.get('temperature', fallback=None)

    def parse_command_line_args(self):
        """从命令行解析参数"""
        parser = argparse.ArgumentParser(description='Read arguments from command line.')
        parser.add_argument('--dbtype', type=str, help='which database you want to connect, it can hive or postgres')
        parser.add_argument('--host', type=str, help='database host which you want to connect, like: '
                                                     '127.0.0.1')
        parser.add_argument('--port', type=str, help='dataBase port which you want to connect, like "5432"')
        parser.add_argument('--user', type=str, help='user for database authentication')
        parser.add_argument('--password', type=str, help='password for database authentication')
        parser.add_argument('--db', type=str, help='database name which you want to connect on')
        parser.add_argument('--baseurl', type=str,
                            help='base url for deepseek LLM, for example: "https://api.deepseek.com"')
        parser.add_argument('--temperature', type=str,
                            help='temperature is used to set LLM sensitivity, the range is 0~1, '
                                 '0 means Lowest sensitivity')
        parser.add_argument('--apikey', type=str, help='api key for deepseek LLM, for example:"sk-xxxxxxxxxxxxxx"')
        parser.add_argument('--model', type=str, help='model name for deepseek, for example:"deepseek-chat"')

        args = parser.parse_args()

        self.dbtype = args.dbtype
        self.host = args.host
        self.port = args.port
        self.user = args.user
        self.password = args.password
        self.db = args.db
        self.baseurl = args.baseurl
        self.apikey = args.apikey
        self.model = args.model
        self.temperature = args.temperature


global_cfg = GlobalConfig()


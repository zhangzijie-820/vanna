import json
import logging
import os
import sys
import uuid
from abc import ABC, abstractmethod
from functools import wraps
import importlib.metadata

import flask
import requests
from flasgger import Swagger
from flask import Flask, Response, jsonify, request, send_from_directory
from flask_sock import Sock

from ..base import VannaBase
from .assets import css_content, html_content, js_content
from .auth import AuthInterface, NoAuth
from src.config.config import global_cfg
from src.config.config import global_superset_api


class Cache(ABC):
    """
    Define the interface for a cache that can be used to store data in a Flask app.
    """

    @abstractmethod
    def generate_id(self, *args, **kwargs):
        """
        Generate a unique ID for the cache.
        """
        pass

    @abstractmethod
    def get(self, id, field):
        """
        Get a value from the cache.
        """
        pass

    @abstractmethod
    def get_all(self, field_list) -> list:
        """
        Get all values from the cache.
        """
        pass

    @abstractmethod
    def set(self, id, field, value):
        """
        Set a value in the cache.
        """
        pass

    @abstractmethod
    def delete(self, id):
        """
        Delete a value from the cache.
        """
        pass


class MemoryCache(Cache):
    def __init__(self):
        self.cache = {}

    def generate_id(self, *args, **kwargs):
        return str(uuid.uuid4())

    def set(self, id, field, value):
        if id not in self.cache:
            self.cache[id] = {}

        self.cache[id][field] = value

    def get(self, id, field):
        if id not in self.cache:
            return None

        if field not in self.cache[id]:
            return None

        return self.cache[id][field]

    def get_all(self, field_list) -> list:
        return [
            {"id": id, **{field: self.get(id=id, field=field) for field in field_list}}
            for id in self.cache
        ]

    def delete(self, id):
        if id in self.cache:
            del self.cache[id]


class VannaFlaskAPI:
    flask_app = None

    def requires_cache(self, required_fields, optional_fields=[]):
        def decorator(f):
            @wraps(f)
            def decorated(*args, **kwargs):
                id = request.args.get("id")

                if id is None:
                    id = request.json.get("id")
                    if id is None:
                        return jsonify({"type": "error", "error": "No id provided"})

                for field in required_fields:
                    if self.cache.get(id=id, field=field) is None:
                        return jsonify({"type": "error", "error": f"No {field} found"})

                field_values = {
                    field: self.cache.get(id=id, field=field) for field in required_fields
                }

                for field in optional_fields:
                    field_values[field] = self.cache.get(id=id, field=field)

                # Add the id to the field_values
                field_values["id"] = id

                return f(*args, **field_values, **kwargs)

            return decorated

        return decorator

    def requires_auth(self, f):
        @wraps(f)
        def decorated(*args, **kwargs):
            user = self.auth.get_user(flask.request)

            if not self.auth.is_logged_in(user):
                return jsonify({"type": "not_logged_in", "html": self.auth.login_form()})

            # Pass the user to the function
            return f(*args, user=user, **kwargs)

        return decorated

    def __init__(
        self,
        vn: VannaBase,
        cache: Cache = MemoryCache(),
        auth: AuthInterface = NoAuth(),
        debug=True,
        allow_llm_to_see_data=False,
        chart=True,
    ):
        """
        Expose a Flask API that can be used to interact with a Vanna instance.

        Args:
            vn: The Vanna instance to interact with.
            cache: The cache to use. Defaults to MemoryCache, which uses an in-memory cache. You can also pass in a custom cache that implements the Cache interface.
            auth: The authentication method to use. Defaults to NoAuth, which doesn't require authentication. You can also pass in a custom authentication method that implements the AuthInterface interface.
            debug: Show the debug console. Defaults to True.
            allow_llm_to_see_data: Whether to allow the LLM to see data. Defaults to False.
            chart: Whether to show the chart output in the UI. Defaults to True.

        Returns:
            None
        """

        self.flask_app = Flask(__name__)

        self.swagger = Swagger(
          self.flask_app, template={"info": {"title": "Vanna API"}}
        )
        self.sock = Sock(self.flask_app)
        self.ws_clients = []
        self.vn = vn
        self.auth = auth
        self.cache = cache
        self.debug = debug
        self.allow_llm_to_see_data = allow_llm_to_see_data
        self.chart = chart
        self.config = {
          "debug": debug,
          "allow_llm_to_see_data": allow_llm_to_see_data,
          "chart": chart,
        }
        log = logging.getLogger("werkzeug")
        log.setLevel(logging.ERROR)

        if "google.colab" in sys.modules:
            self.debug = False
            print("Google Colab doesn't support running websocket servers. Disabling debug mode.")

        if self.debug:
            def log(message, title="Info"):
                [ws.send(json.dumps({'message': message, 'title': title})) for ws in self.ws_clients]

            self.vn.log = log

        @self.flask_app.route("/api/v0/get_config", methods=["GET"])
        @self.requires_auth
        def get_config(user: any):
            """
            Get the configuration for a user
            ---
            parameters:
              - name: user
                in: query
            responses:
              200:
                schema:
                  type: object
                  properties:
                    type:
                      type: string
                      default: config
                    config:
                      type: object
            """
            config = self.auth.override_config_for_user(user, self.config)
            return jsonify(
                {
                    "type": "config",
                    "config": config
                }
            )

        @self.flask_app.route("/api/v0/generate_questions", methods=["GET"])
        @self.requires_auth
        def generate_questions(user: any):
            """
            Generate questions
            ---
            parameters:
              - name: user
                in: query
            responses:
              200:
                schema:
                  type: object
                  properties:
                    type:
                      type: string
                      default: question_list
                    questions:
                      type: array
                      items:
                        type: string
                    header:
                      type: string
                      default: Here are some questions you can ask
            """
            # If self has an _model attribute and model=='chinook'
            if hasattr(self.vn, "_model") and self.vn._model == "chinook":
                return jsonify(
                    {
                        "type": "question_list",
                        "questions": [
                            "What are the top 10 artists by sales?",
                            "What are the total sales per year by country?",
                            "Who is the top selling artist in each genre? Show the sales numbers.",
                            "How do the employees rank in terms of sales performance?",
                            "Which 5 cities have the most customers?",
                        ],
                        "header": "Here are some questions you can ask:",
                    }
                )

            training_data = vn.get_training_data()

            # If training data is None or empty
            if training_data is None or len(training_data) == 0:
                return jsonify(
                    {
                        "type": "error",
                        "error": "No training data found. Please add some training data first.",
                    }
                )

            # Get the questions from the training data
            try:
                # Filter training data to only include questions where the question is not null
                questions = (
                    training_data[training_data["question"].notnull()]
                    .sample(5)["question"]
                    .tolist()
                )

                # Temporarily this will just return an empty list
                return jsonify(
                    {
                        "type": "question_list",
                        "questions": questions,
                        "header": "Here are some questions you can ask",
                    }
                )
            except Exception as e:
                return jsonify(
                    {
                        "type": "question_list",
                        "questions": [],
                        "header": "Go ahead and ask a question",
                    }
                )

        @self.flask_app.route("/api/v0/generate_sql", methods=["GET"])
        @self.requires_auth
        def generate_sql(user: any):
            """
            Generate SQL from a question
            ---
            parameters:
              - name: user
                in: query
              - name: question
                in: query
                type: string
                required: true
            responses:
              200:
                schema:
                  type: object
                  properties:
                    type:
                      type: string
                      default: sql
                    id:
                      type: string
                    text:
                      type: string
            """
            question = flask.request.args.get("question")

            if question is None:
                return jsonify({"type": "error", "error": "No question provided"})

            id = self.cache.generate_id(question=question)
            sql = vn.generate_sql(question=question, allow_llm_to_see_data=self.allow_llm_to_see_data)

            self.cache.set(id=id, field="question", value=question)
            self.cache.set(id=id, field="sql", value=sql)

            if vn.is_sql_valid(sql=sql):
                return jsonify(
                    {
                        "type": "sql",
                        "id": id,
                        "text": sql,
                    }
                )
            else:
                return jsonify(
                    {
                        "type": "text",
                        "id": id,
                        "text": sql,
                    }
                )

        @self.flask_app.route("/api/v0/generate_rewritten_question", methods=["GET"])
        @self.requires_auth
        def generate_rewritten_question(user: any):
            """
            Generate a rewritten question
            ---
            parameters:
              - name: last_question
                in: query
                type: string
                required: true
              - name: new_question
                in: query
                type: string
                required: true
            """

            last_question = flask.request.args.get("last_question")
            new_question = flask.request.args.get("new_question")

            rewritten_question = self.vn.generate_rewritten_question(last_question, new_question)

            return jsonify({"type": "rewritten_question", "question": rewritten_question})

        @self.flask_app.route("/api/v0/get_function", methods=["GET"])
        @self.requires_auth
        def get_function(user: any):
            """
            Get a function from a question
            ---
            parameters:
              - name: user
                in: query
              - name: question
                in: query
                type: string
                required: true
            responses:
              200:
                schema:
                  type: object
                  properties:
                    type:
                      type: string
                      default: function
                    id:
                      type: object
                    function:
                      type: string
            """
            question = flask.request.args.get("question")

            if question is None:
                return jsonify({"type": "error", "error": "No question provided"})

            if not hasattr(vn, "get_function"):
                return jsonify({"type": "error", "error": "This setup does not support function generation."})

            id = self.cache.generate_id(question=question)
            function = vn.get_function(question=question)

            if function is None:
                return jsonify({"type": "error", "error": "No function found"})

            if 'instantiated_sql' not in function:
                self.vn.log(f"No instantiated SQL found for {question} in {function}")
                return jsonify({"type": "error", "error": "No instantiated SQL found"})

            self.cache.set(id=id, field="question", value=question)
            self.cache.set(id=id, field="sql", value=function['instantiated_sql'])

            if 'instantiated_post_processing_code' in function and function['instantiated_post_processing_code'] is not None and len(function['instantiated_post_processing_code']) > 0:
                self.cache.set(id=id, field="plotly_code", value=function['instantiated_post_processing_code'])

            return jsonify(
                {
                    "type": "function",
                    "id": id,
                    "function": function,
                }
            )

        @self.flask_app.route("/api/v0/get_all_functions", methods=["GET"])
        @self.requires_auth
        def get_all_functions(user: any):
            """
            Get all the functions
            ---
            parameters:
              - name: user
                in: query
            responses:
              200:
                schema:
                  type: object
                  properties:
                    type:
                      type: string
                      default: functions
                    functions:
                      type: array
            """
            if not hasattr(vn, "get_all_functions"):
                return jsonify({"type": "error", "error": "This setup does not support function generation."})

            functions = vn.get_all_functions()

            return jsonify(
                {
                    "type": "functions",
                    "functions": functions,
                }
            )

        @self.flask_app.route("/api/v0/run_sql", methods=["GET"])
        @self.requires_auth
        @self.requires_cache(["sql"])
        def run_sql(user: any, id: str, sql: str):
            """
            Run SQL
            ---
            parameters:
              - name: user
                in: query
              - name: id
                in: query|body
                type: string
                required: true
            responses:
              200:
                schema:
                  type: object
                  properties:
                    type:
                      type: string
                      default: df
                    id:
                      type: string
                    df:
                      type: object
                    should_generate_chart:
                      type: boolean
            """
            try:
                if not vn.run_sql_is_set:
                    return jsonify(
                        {
                            "type": "error",
                            "error": "Please connect to a database using vn.connect_to_... in order to run SQL queries.",
                        }
                    )

                df = vn.run_sql(sql=sql)

                self.cache.set(id=id, field="df", value=df)

                return jsonify(
                    {
                        "type": "df",
                        "id": id,
                        "df": df.head(10).to_json(orient='records', date_format='iso'),
                        "should_generate_chart": self.chart and vn.should_generate_chart(df),
                    }
                )

            except Exception as e:
                return jsonify({"type": "sql_error", "error": str(e)})

        @self.flask_app.route("/api/v0/fix_sql", methods=["POST"])
        @self.requires_auth
        @self.requires_cache(["question", "sql"])
        def fix_sql(user: any, id: str, question: str, sql: str):
            """
            Fix SQL
            ---
            parameters:
              - name: user
                in: query
              - name: id
                in: query|body
                type: string
                required: true
              - name: error
                in: body
                type: string
                required: true
            responses:
              200:
                schema:
                  type: object
                  properties:
                    type:
                      type: string
                      default: sql
                    id:
                      type: string
                    text:
                      type: string
            """
            error = flask.request.json.get("error")

            if error is None:
                return jsonify({"type": "error", "error": "No error provided"})

            question = f"I have an error: {error}\n\nHere is the SQL I tried to run: {sql}\n\nThis is the question I was trying to answer: {question}\n\nCan you rewrite the SQL to fix the error?"

            fixed_sql = vn.generate_sql(question=question)

            self.cache.set(id=id, field="sql", value=fixed_sql)

            return jsonify(
                {
                    "type": "sql",
                    "id": id,
                    "text": fixed_sql,
                }
            )


        @self.flask_app.route('/api/v0/update_sql', methods=['POST'])
        @self.requires_auth
        @self.requires_cache([])
        def update_sql(user: any, id: str):
            """
            Update SQL
            ---
            parameters:
              - name: user
                in: query
              - name: id
                in: query|body
                type: string
                required: true
              - name: sql
                in: body
                type: string
                required: true
            responses:
              200:
                schema:
                  type: object
                  properties:
                    type:
                      type: string
                      default: sql
                    id:
                      type: string
                    text:
                      type: string
            """
            sql = flask.request.json.get('sql')

            if sql is None:
                return jsonify({"type": "error", "error": "No sql provided"})

            self.cache.set(id=id, field='sql', value=sql)

            return jsonify(
                {
                    "type": "sql",
                    "id": id,
                    "text": sql,
                })

        @self.flask_app.route("/api/v0/download_csv", methods=["GET"])
        @self.requires_auth
        @self.requires_cache(["df"])
        def download_csv(user: any, id: str, df):
            """
            Download CSV
            ---
            parameters:
              - name: user
                in: query
              - name: id
                in: query|body
                type: string
                required: true
            responses:
              200:
                description: download CSV
            """
            csv = df.to_csv()

            return Response(
                csv,
                mimetype="text/csv",
                headers={"Content-disposition": f"attachment; filename={id}.csv"},
            )

        @self.flask_app.route("/api/v0/generate_plotly_figure", methods=["GET"])
        @self.requires_auth
        @self.requires_cache(["df", "question", "sql"])
        def generate_plotly_figure(user: any, id: str, df, question, sql):
            """
            Generate plotly figure
            ---
            parameters:
              - name: user
                in: query
              - name: id
                in: query|body
                type: string
                required: true
              - name: chart_instructions
                in: body
                type: string
            responses:
              200:
                schema:
                  type: object
                  properties:
                    type:
                      type: string
                      default: plotly_figure
                    id:
                      type: string
                    fig:
                      type: object
            """
            chart_instructions = flask.request.args.get('chart_instructions')

            try:
                # If chart_instructions is not set then attempt to retrieve the code from the cache
                if chart_instructions is None or len(chart_instructions) == 0:
                    code = self.cache.get(id=id, field="plotly_code")
                else:
                    question = f"{question}. When generating the chart, use these special instructions: {chart_instructions}"
                    code = vn.generate_plotly_code(
                        question=question,
                        sql=sql,
                        df_metadata=f"Running df.dtypes gives:\n {df.dtypes}",
                    )
                    self.cache.set(id=id, field="plotly_code", value=code)

                fig = vn.get_plotly_figure(plotly_code=code, df=df, dark_mode=False)
                fig_json = fig.to_json()

                self.cache.set(id=id, field="fig_json", value=fig_json)

                return jsonify(
                    {
                        "type": "plotly_figure",
                        "id": id,
                        "fig": fig_json,
                    }
                )
            except Exception as e:
                # Print the stack trace
                import traceback

                traceback.print_exc()

                return jsonify({"type": "error", "error": str(e)})

        @self.flask_app.route("/api/v0/get_training_data", methods=["GET"])
        @self.requires_auth
        def get_training_data(user: any):
            """
            Get all training data
            ---
            parameters:
              - name: user
                in: query
            responses:
              200:
                schema:
                  type: object
                  properties:
                    type:
                      type: string
                      default: df
                    id:
                      type: string
                      default: training_data
                    df:
                      type: object
            """
            df = vn.get_training_data()

            if df is None or len(df) == 0:
                return jsonify(
                    {
                        "type": "error",
                        "error": "No training data found. Please add some training data first.",
                    }
                )

            return jsonify(
                {
                    "type": "df",
                    "id": "training_data",
                    "df": df.to_json(orient="records"),
                }
            )

        @self.flask_app.route("/api/v0/remove_training_data", methods=["POST"])
        @self.requires_auth
        def remove_training_data(user: any):
            """
            Remove training data
            ---
            parameters:
              - name: user
                in: query
              - name: id
                in: body
                type: string
                required: true
            responses:
              200:
                schema:
                  type: object
                  properties:
                    success:
                      type: boolean
            """
            # Get id from the JSON body
            id = flask.request.json.get("id")

            if id is None:
                return jsonify({"type": "error", "error": "No id provided"})

            if vn.remove_training_data(id=id):
                return jsonify({"success": True})
            else:
                return jsonify(
                    {"type": "error", "error": "Couldn't remove training data"}
                )

        @self.flask_app.route("/api/v0/train", methods=["POST"])
        @self.requires_auth
        def add_training_data(user: any):
            """
            Add training data
            ---
            parameters:
              - name: user
                in: query
              - name: question
                in: body
                type: string
              - name: sql
                in: body
                type: string
              - name: ddl
                in: body
                type: string
              - name: documentation
                in: body
                type: string
            responses:
              200:
                schema:
                  type: object
                  properties:
                    id:
                      type: string
            """
            question = flask.request.json.get("question")
            sql = flask.request.json.get("sql")
            ddl = flask.request.json.get("ddl")
            documentation = flask.request.json.get("documentation")

            try:
                id = vn.train(
                    question=question, sql=sql, ddl=ddl, documentation=documentation
                )

                return jsonify({"id": id})
            except Exception as e:
                print("TRAINING ERROR", e)
                return jsonify({"type": "error", "error": str(e)})

        @self.flask_app.route("/api/v0/create_function", methods=["GET"])
        @self.requires_auth
        @self.requires_cache(["question", "sql"])
        def create_function(user: any, id: str, question: str, sql: str):
            """
            Create function
            ---
            parameters:
              - name: user
                in: query
              - name: id
                in: query|body
                type: string
                required: true
            responses:
              200:
                schema:
                  type: object
                  properties:
                    type:
                      type: string
                      default: function_template
                    id:
                      type: string
                    function_template:
                      type: object
            """
            plotly_code = self.cache.get(id=id, field="plotly_code")

            if plotly_code is None:
                plotly_code = ""

            function_data = self.vn.create_function(question=question, sql=sql, plotly_code=plotly_code)

            return jsonify(
                {
                    "type": "function_template",
                    "id": id,
                    "function_template": function_data,
                }
            )

        @self.flask_app.route("/api/v0/update_function", methods=["POST"])
        @self.requires_auth
        def update_function(user: any):
            """
            Update function
            ---
            parameters:
              - name: user
                in: query
              - name: old_function_name
                in: body
                type: string
                required: true
              - name: updated_function
                in: body
                type: object
                required: true
            responses:
              200:
                schema:
                  type: object
                  properties:
                    success:
                      type: boolean
            """
            old_function_name = flask.request.json.get("old_function_name")
            updated_function = flask.request.json.get("updated_function")

            print("old_function_name", old_function_name)
            print("updated_function", updated_function)

            updated = vn.update_function(old_function_name=old_function_name, updated_function=updated_function)

            return jsonify({"success": updated})

        @self.flask_app.route("/api/v0/delete_function", methods=["POST"])
        @self.requires_auth
        def delete_function(user: any):
            """
            Delete function
            ---
            parameters:
              - name: user
                in: query
              - name: function_name
                in: body
                type: string
                required: true
            responses:
              200:
                schema:
                  type: object
                  properties:
                    success:
                      type: boolean
            """
            function_name = flask.request.json.get("function_name")

            return jsonify({"success": vn.delete_function(function_name=function_name)})

        @self.flask_app.route("/api/v0/generate_followup_questions", methods=["GET"])
        @self.requires_auth
        @self.requires_cache(["df", "question", "sql"])
        def generate_followup_questions(user: any, id: str, df, question, sql):
            """
            Generate followup questions
            ---
            parameters:
              - name: user
                in: query
              - name: id
                in: query|body
                type: string
                required: true
            responses:
              200:
                schema:
                  type: object
                  properties:
                    type:
                      type: string
                      default: question_list
                    questions:
                      type: array
                      items:
                        type: string
                    header:
                      type: string
            """
            if self.allow_llm_to_see_data:
                followup_questions = vn.generate_followup_questions(
                    question=question, sql=sql, df=df
                )
                if followup_questions is not None and len(followup_questions) > 5:
                    followup_questions = followup_questions[:5]

                self.cache.set(id=id, field="followup_questions", value=followup_questions)

                return jsonify(
                    {
                        "type": "question_list",
                        "id": id,
                        "questions": followup_questions,
                        "header": "Here are some potential followup questions:",
                    }
                )
            else:
                self.cache.set(id=id, field="followup_questions", value=[])
                return jsonify(
                    {
                        "type": "question_list",
                        "id": id,
                        "questions": [],
                        "header": "Followup Questions can be enabled if you set allow_llm_to_see_data=True",
                    }
                )

        @self.flask_app.route("/api/v0/create_dataset", methods=["POST"])
        @self.requires_auth
        # 入口: superset的相关信息，database_id，schema和table_name
        # 出口： dataset_id
        def create_dataset(user: any):
            db_id = flask.request.json.get("database_id")
            schema = flask.request.json.get("schema")
            table_name = flask.request.json.get("table_name")

            db_set_id, error = global_superset_api.get_dataset_with_name(table_name, schema, db_id)
            if db_set_id is not None:
                return jsonify(
                    {
                        "type": "txt",
                        "txt": f"{db_set_id}"
                    }
                )
            elif db_set_id is None and error is None:
                db_set_id, error = global_superset_api.create_dataset_with_table(db_id, schema, table_name)
                if db_set_id is not None:
                    return jsonify(
                        {
                            "type": "txt",
                            "txt": f"{db_set_id}",
                        }
                    )
                else:
                    return jsonify(
                        {
                            "type": "error",
                            "error": error,
                        }
                    )
            elif db_set_id is None and error is not None:
                return jsonify(
                    {
                        "type": "error",
                        "error": error
                    }
                )

        @self.flask_app.route("/api/v0/create_db", methods=["POST"])
        @self.requires_auth
        # 入口: superset的相关信息
        # 出口： database_id
        def create_db(user: any):
            databases = f"{global_cfg.dbtype}://{global_cfg.user}:{global_cfg.password}@{global_cfg.host}:{global_cfg.port}"
            if global_cfg.dbtype == "hive":
                databases = databases + "/?auth=CUSTOM"
            else:
                databases = databases + f"/{global_cfg.db}"

            print(f"create database url is {databases}")

            db_id, error_msg = global_superset_api.get_database_with_name(databases)
            if db_id is not None:
                return jsonify(
                    {
                        "type": "txt",
                        "txt": f"{db_id}",
                    }
                )
            elif db_id is None and error_msg is None:
                db_id, error_msg = global_superset_api.create_database(databases)
                if db_id is not None:
                    return jsonify(
                        {
                            "type": "txt",
                            "txt": f"{db_id}",
                        }
                    )
                else:
                    return jsonify(
                        {
                             "type": "error",
                             "error": error_msg,
                        }
                    )
            elif db_id is None and error_msg is not None:
                return jsonify(
                    {
                        "type": "error",
                        "error": error_msg

                    }
                )

        @self.flask_app.route("/api/v0/login_superset", methods=["POST"])
        # 入口: superset的相关信息
        # 出口： token
        @self.requires_auth
        def login_superset(user: any):
            access_token, error_msg = global_superset_api.login()
            if access_token:
                return jsonify(
                    {
                        "type": "text",
                        "text": access_token,
                    }
                )
            else:
                return jsonify(
                    {
                        "type": "error",
                        "error": error_msg,
                    }
                )

        @self.flask_app.route("/api/v0/export_info", methods=["POST"])
        @self.requires_auth
        # 入口: LLM吐出来的指标，维度信息
        # 出口： 整理过后的指标，维度信息，后续在agent builder上调用对应的API获取dataset id
        def export_info(user: any):
            dataset_id = flask.request.args.get("dataset_id")
            schema_name = flask.request.json.get("schema_name")
            table_name = flask.request.json.get("table_name")
            chart_type = flask.request.json.get("chart_type")
            desc = flask.request.json.get("desc")
            limit = flask.request.json.get("limit")
            filters = flask.request.json.get("filters")
            order_by = flask.request.json.get("order_by")

            metrics = [metric['column_name'] for metric in flask.request.json['metrics']]
            metrics_str = ', '.join(metrics)

            dimensions = [dimension['column_name'] for dimension in flask.request.json['dimensions']]
            dimensions_str = ', '.join(dimensions)

            if "column_name" in filters:
                where_clause = filters["column_name"]
            else:
                where_clause = ""

            if "column_name" in order_by:
                order_by_clause = order_by["column_name"]
            else:
                order_by_clause = ""

            return jsonify(
                {
                    "schema_name": schema_name,
                    "table_name": table_name,
                    "chart_type": chart_type,
                    "metric": metrics_str,
                    "dimension": dimensions_str,
                    "desc": desc,
                    "limit": limit,
                    "order_by": order_by_clause,
                    "filters": where_clause,
                    "datasource_type": "table",
                    "dataset_id": dataset_id
                }
            )

        @self.flask_app.route("/api/v0/get_sql_from_superset", methods=["POST"])
        @self.requires_auth
        def get_sql_from_superset(user: any):
            dataset_id = flask.request.args.get("dataset_id")
            filters = flask.request.json.get("filters")
            metrics = flask.request.json.get("metrics")
            order_by = flask.request.json.get("order_by")
            dimensions = flask.request.json.get("dimensions")

            limit = flask.request.json.get("limit")
            limit = int(limit) if limit else 50000

            desc = flask.request.json.get("desc")
            desc_bool = desc.lower() == "true" if desc else False

            if filters and "column_name" in filters:
                where_clause = filters["column_name"]
            else:
                where_clause = ""

            if order_by and "column_name" in order_by:
                order_by_clause = order_by["column_name"]
            else:
                order_by_clause = ""
                order_by = []

            output_data = {
                "datasource": {
                    "id": dataset_id,
                    "type": "table"
                },
                "queries": [
                    {
                         "extras": {
                         "where": where_clause
                    },
                    "metrics": [
                        {
                              "expressionType": "SQL",
                              "sqlExpression": metric["column_name"],
                               "label": metric["alis"]
                        } for metric in metrics
                    ],
                    "orderby": [
                        [
                            {
                                "expressionType": "SQL",
                                 "sqlExpression": order_by_clause,
                           },
                           desc_bool
                        ]
                     ] if order_by else [],
                     "groupby": [
                          {
                              "expressionType": "SQL",
                              "sqlExpression": dimension["column_name"],
                          } for dimension in dimensions
                     ],
                     "row_limit": int(limit)
                    }
                ],
                "result_format": "json",
                 "result_type": "full"
            }

            sql, error_msg = global_superset_api.get_sql_and_data_from_superset(output_data)
            if error_msg is not None:
                return jsonify(
                    {
                        "type": "error",
                         "error": error_msg,
                    }
                )
            else:
                return jsonify(
                    {
                        "type": "sql",
                        "sql": sql,
                    }
                )

        @self.flask_app.route("/api/v0/generate_dimension", methods=["GET"])
        # 入口: 自然语言
        # 出口： 大模型吐出来的指标和维度信息，有缓存，可以获取历史问题以及对应的维度和指标信息
        @self.requires_auth
        def generate_dimension(user: any):
            question = flask.request.args.get("question")

            if question is None:
                return jsonify({"type": "error",
                                "error": "No question provided"})

            id = self.cache.generate_id(question=question)
            dimension = vn.generate_dimension(question=question)

            self.cache.set(id=id, field="question", value=question)
            self.cache.set(id=id, field="dimension", value=dimension)

            if dimension == "":
                return jsonify(
                    {
                        "type": "text",
                        "id": id,
                        "text": "no dimension and metric found",
                    }
                )
            else:
                return jsonify(
                  {
                      "type": "json",
                      "id": id,
                      "json": dimension,
                  }
                )

        @self.flask_app.route("/api/v0/generate_summary", methods=["GET"])
        @self.requires_auth
        @self.requires_cache(["df", "question"])
        def generate_summary(user: any, id: str, df, question):
            """
            Generate summary
            ---
            parameters:
              - name: user
                in: query
              - name: id
                in: query|body
                type: string
                required: true
            responses:
              200:
                schema:
                  type: object
                  properties:
                    type:
                      type: string
                      default: text
                    id:
                      type: string
                    text:
                      type: string
            """
            if self.allow_llm_to_see_data:
                summary = vn.generate_summary(question=question, df=df)

                self.cache.set(id=id, field="summary", value=summary)

                return jsonify(
                    {
                        "type": "text",
                        "id": id,
                        "text": summary,
                    }
                )
            else:
                return jsonify(
                    {
                        "type": "text",
                        "id": id,
                        "text": "Summarization can be enabled if you set allow_llm_to_see_data=True",
                    }
                )

        @self.flask_app.route("/api/v0/load_dimension", methods=["GET"])
        @self.requires_auth
        @self.requires_cache(
          ["question"],
          optional_fields=["dimension"]
        )
        def load_dimension(user: any, id: str, question, dimension):
            try:
                return jsonify(
                    {
                        "type": "question_cache",
                        "id": id,
                        "question": question,
                        "dimension": dimension,
                    }
                )
            except Exception as e:
                return jsonify({"type": "error", "error": str(e)})

        @self.flask_app.route("/api/v0/load_question", methods=["GET"])
        @self.requires_auth
        @self.requires_cache(
            ["question", "sql", "df"],
            optional_fields=["summary", "fig_json"]
        )
        def load_question(user: any, id: str, question, sql, df, fig_json, summary):
            """
            Load question
            ---
            parameters:
              - name: user
                in: query
              - name: id
                in: query|body
                type: string
                required: true
            responses:
              200:
                schema:
                  type: object
                  properties:
                    type:
                      type: string
                      default: question_cache
                    id:
                      type: string
                    question:
                      type: string
                    sql:
                      type: string
                    df:
                      type: object
                    fig:
                      type: object
                    summary:
                      type: string
            """
            try:
                return jsonify(
                    {
                        "type": "question_cache",
                        "id": id,
                        "question": question,
                        "sql": sql,
                        "df": df.head(10).to_json(orient="records", date_format="iso"),
                        "fig": fig_json,
                        "summary": summary,
                    }
                )

            except Exception as e:
                return jsonify({"type": "error", "error": str(e)})

        @self.flask_app.route("/api/v0/get_question_history", methods=["GET"])
        @self.requires_auth
        def get_question_history(user: any):
            """
            Get question history
            ---
            parameters:
              - name: user
                in: query
            responses:
              200:
                schema:
                  type: object
                  properties:
                    type:
                      type: string
                      default: question_history
                    questions:
                      type: array
                      items:
                        type: string
            """
            return jsonify(
                {
                    "type": "question_history",
                    "questions": cache.get_all(field_list=["question"]),
                }
            )

        @self.flask_app.route("/api/v0/<path:catch_all>", methods=["GET", "POST"])
        def catch_all(catch_all):
            return jsonify(
                {"type": "error", "error": "The rest of the API is not ported yet."}
            )

        if self.debug:
            @self.sock.route("/api/v0/log")
            def sock_log(ws):
                self.ws_clients.append(ws)

                try:
                    while True:
                        message = ws.receive()  # This example just reads and ignores to keep the socket open
                finally:
                    self.ws_clients.remove(ws)

    def run(self, *args, **kwargs):
        """
        Run the Flask app.

        Args:
            *args: Arguments to pass to Flask's run method.
            **kwargs: Keyword arguments to pass to Flask's run method.

        Returns:
            None
        """
        if args or kwargs:
            self.flask_app.run(*args, **kwargs)

        else:
            try:
                from google.colab import output

                output.serve_kernel_port_as_window(8084)
                from google.colab.output import eval_js

                print("Your app is running at:")
                print(eval_js("google.colab.kernel.proxyPort(8084)"))
            except:
                print("Your app is running at:")
                print("http://localhost:8084")

            self.flask_app.run(host="0.0.0.0", port=8084, debug=self.debug, use_reloader=False)


class VannaFlaskApp(VannaFlaskAPI):
    def __init__(
        self,
        vn: VannaBase,
        cache: Cache = MemoryCache(),
        auth: AuthInterface = NoAuth(),
        debug=True,
        allow_llm_to_see_data=False,
        logo="https://img.vanna.ai/vanna-flask.svg",
        title="Welcome to Vanna.AI",
        subtitle="Your AI-powered copilot for SQL queries.",
        show_training_data=True,
        suggested_questions=True,
        sql=True,
        table=True,
        csv_download=True,
        chart=True,
        redraw_chart=True,
        auto_fix_sql=True,
        ask_results_correct=True,
        followup_questions=True,
        summarization=True,
        function_generation=True,
        index_html_path=None,
        assets_folder=None,
    ):
        """
        Expose a Flask app that can be used to interact with a Vanna instance.

        Args:
            vn: The Vanna instance to interact with.
            cache: The cache to use. Defaults to MemoryCache, which uses an in-memory cache. You can also pass in a custom cache that implements the Cache interface.
            auth: The authentication method to use. Defaults to NoAuth, which doesn't require authentication. You can also pass in a custom authentication method that implements the AuthInterface interface.
            debug: Show the debug console. Defaults to True.
            allow_llm_to_see_data: Whether to allow the LLM to see data. Defaults to False.
            logo: The logo to display in the UI. Defaults to the Vanna logo.
            title: The title to display in the UI. Defaults to "Welcome to Vanna.AI".
            subtitle: The subtitle to display in the UI. Defaults to "Your AI-powered copilot for SQL queries.".
            show_training_data: Whether to show the training data in the UI. Defaults to True.
            suggested_questions: Whether to show suggested questions in the UI. Defaults to True.
            sql: Whether to show the SQL input in the UI. Defaults to True.
            table: Whether to show the table output in the UI. Defaults to True.
            csv_download: Whether to allow downloading the table output as a CSV file. Defaults to True.
            chart: Whether to show the chart output in the UI. Defaults to True.
            redraw_chart: Whether to allow redrawing the chart. Defaults to True.
            auto_fix_sql: Whether to allow auto-fixing SQL errors. Defaults to True.
            ask_results_correct: Whether to ask the user if the results are correct. Defaults to True.
            followup_questions: Whether to show followup questions. Defaults to True.
            summarization: Whether to show summarization. Defaults to True.
            index_html_path: Path to the index.html. Defaults to None, which will use the default index.html
            assets_folder: The location where you'd like to serve the static assets from. Defaults to None, which will use hardcoded Python variables.

        Returns:
            None
        """
        super().__init__(vn, cache, auth, debug, allow_llm_to_see_data, chart)

        self.config["logo"] = logo
        self.config["title"] = title
        self.config["subtitle"] = subtitle
        self.config["show_training_data"] = show_training_data
        self.config["suggested_questions"] = suggested_questions
        self.config["sql"] = sql
        self.config["table"] = table
        self.config["csv_download"] = csv_download
        self.config["chart"] = chart
        self.config["redraw_chart"] = redraw_chart
        self.config["auto_fix_sql"] = auto_fix_sql
        self.config["ask_results_correct"] = ask_results_correct
        self.config["followup_questions"] = followup_questions
        self.config["summarization"] = summarization
        self.config["function_generation"] = function_generation and hasattr(vn, "get_function")
        self.config["version"] = importlib.metadata.version('vanna')

        self.index_html_path = index_html_path
        self.assets_folder = assets_folder

        @self.flask_app.route("/auth/login", methods=["POST"])
        def login():
            return self.auth.login_handler(flask.request)

        @self.flask_app.route("/auth/callback", methods=["GET"])
        def callback():
            return self.auth.callback_handler(flask.request)

        @self.flask_app.route("/auth/logout", methods=["GET"])
        def logout():
            return self.auth.logout_handler(flask.request)


        @self.flask_app.route("/assets/<path:filename>")
        def proxy_assets(filename):
            if self.assets_folder:
                return send_from_directory(self.assets_folder, filename)

            if ".css" in filename:
                return Response(css_content, mimetype="text/css")

            if ".js" in filename:
                return Response(js_content, mimetype="text/javascript")

            # Return 404
            return "File not found", 404

        # Proxy the /vanna.svg file to the remote server
        @self.flask_app.route("/vanna.svg")
        def proxy_vanna_svg():
            remote_url = "https://vanna.ai/img/vanna.svg"
            response = requests.get(remote_url, stream=True)

            # Check if the request to the remote URL was successful
            if response.status_code == 200:
                excluded_headers = [
                    "content-encoding",
                    "content-length",
                    "transfer-encoding",
                    "connection",
                ]
                headers = [
                    (name, value)
                    for (name, value) in response.raw.headers.items()
                    if name.lower() not in excluded_headers
                ]
                return Response(response.content, response.status_code, headers)
            else:
                return "Error fetching file from remote server", response.status_code

        @self.flask_app.route("/", defaults={"path": ""})
        @self.flask_app.route("/<path:path>")
        def hello(path: str):
            if self.index_html_path:
                directory = os.path.dirname(self.index_html_path)
                filename = os.path.basename(self.index_html_path)
                return send_from_directory(directory=directory, path=filename)
            return html_content

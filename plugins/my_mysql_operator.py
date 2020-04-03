from airflow.models.baseoperator import BaseOperator
from airflow.utils.decorators import apply_defaults
import logging
from airflow.plugins_manager import AirflowPlugin
# from airflow.hooks.mysql_hook import MySqlHook
class HelloOperator(BaseOperator):
    @apply_defaults
    def __init__(
        self,
        name:str,
        *args, **kwargs)->None:
        super().__init__(*args, **kwargs)
        self.name = name
    def execute(self, context):
        message = "Hello {}".format(self.name)
        logging.info(message)
        return message

# class MyMysqlOperator(BaseOperator):
#     @apply_defaults
#     def __init__(
#         self,
#         name:str,
#         mysql_conn_id:str,
#         *args, **kwargs)->None:
#         super().__init__(*args, **kwargs)
#         self.name = name
#         self.mysql_conn_id = mysql_conn_id
#     def execute(self, context):
#         hook = MySqlHook(mysql_conn_id = self.mysql_conn_id)
#         sql = "select name from test"
#         result = hook.get_first(sql)
#         message = "Hello Mysql {}".format(result["name"])
#         logging.info(message)
#         return message
class MyMysqlOperator(BaseOperator):
    @apply_defaults
    def __init__(
        self,
        name:str,
        mysql_conn_id:str,
        *args, **kwargs)->None:
        super().__init__(*args, **kwargs)
        self.name = name
        self.mysql_conn_id = mysql_conn_id
    def execute(self, context):
        # hook = MySqlHook(mysql_conn_id = self.mysql_conn_id)
        # sql = "select name from test"
        # result = hook.get_first(sql)
        result = {"name":"fanzhen"}
        message = "Hello Mysql {}".format(result["name"])
        logging.info(message)
        return message

# Creating the REST_API_Plugin which extends the AirflowPlugin so its imported into Airflow
class My_First_Plugin(AirflowPlugin):
    name = "my_mysql_operator"
    operators = [MyMysqlOperator, HelloOperator]
    appbuilder_views = []
    flask_blueprints = []
    hooks = []
    executors = []
    admin_views = []
    menu_links = []
from airflow.plugins_manager import AirflowPlugin
from mongo_plugin.hooks.mongo_hook import MongoHook
from mongo_plugin.operators.s3_to_mongo_operator import S3ToMongoOperator
from mongo_plugin.operators.mongo_to_s3_operator import MongoToS3Operator


class MongoPlugin(AirflowPlugin):
    name = "MongoPlugin"
    operators = [MongoToS3Operator, S3ToMongoOperator]
    hooks = [MongoHook]
    executors = []
    macros = []
    admin_views = []
    flask_blueprints = []
    menu_links = []

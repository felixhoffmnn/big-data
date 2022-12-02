from airflow.plugins_manager import AirflowPlugin
from operators.hdfs_file_operator import HdfsMkdirsFileOperator, HdfsPutFilesOperator

class HdfsPlugin(AirflowPlugin):
    name = "hdfs_plugins"
    operators = [HdfsMkdirsFileOperator, HdfsPutFilesOperator]
    hooks = []
    executors = []
    macros = []
    admin_views = []
    flask_blueprints = []
    menu_links = []

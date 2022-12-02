from airflow.plugins_manager import AirflowPlugin
from operators.directory_operator import ClearDirectoryOperator, CreateDirectoryOperator


class DirectoryPlugin(AirflowPlugin):
    name = "directory_plugins"
    operators = [CreateDirectoryOperator, ClearDirectoryOperator]
    hooks = []
    executors = []
    macros = []
    admin_views = []
    flask_blueprints = []
    menu_links = []

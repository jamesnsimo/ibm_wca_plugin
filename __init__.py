
from airflow.plugins_manager import AirflowPlugin
from ibm_wca_plugin.hooks.ibm_wca_hook import IbmWcaHook

from ibm_wca_plugin.operators.ibm_wca_export_list_to_gcs_operator import (
    IbmWcaExportListToGCSOperator
)


class IbmWcaPlugin(AirflowPlugin):
    name = "ibm_wca_plugin"
    operators = [IbmWcaExportListToGCSOperator]
    hooks = [IbmWcaHook]

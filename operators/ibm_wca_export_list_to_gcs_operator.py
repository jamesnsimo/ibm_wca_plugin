from airflow.models import BaseOperator

from airflow.utils.decorators import apply_defaults

from ibm_wca_plugin.hooks.ibm_wca_hook import IbmWcaHook
from airflow.contrib.hooks.gcs_hook import GoogleCloudStorageHook


class IbmWcaExportListToGCSOperator(BaseOperator):
    @apply_defaults
    def __init__(self, ibm_wca_conn_id, gcs_conn_id, list_id, gcs_bucket, gcs_key, **kwargs):
        super().__init__(**kwargs)
        self.gcs_conn_id = gcs_conn_id
        self.ibm_wca_conn_id = ibm_wca_conn_id
        self.list_id = list_id
        self.gcs_bucket = gcs_bucket
        self.gcs_key = gcs_key
        self.local_key = "/home/airflow/gcs/data/{}".format(gcs_key)
        self.ibm_hook = IbmWcaHook(self.ibm_wca_conn_id)
        self.gcs_hook = GoogleCloudStorageHook(self.gcs_conn_id)

    def execute(self, context):
        job = self.ibm_hook.export_list(list_id=self.list_id)
        self.job_id = job["JOB_ID"]
        self.ftp_path = job["FILE_PATH"]
        self.ibm_hook.poll_job_status(job_id=self.job_id, wait=10)
        self.ibm_hook.ftp_get(self.ftp_path, self.gcs_key)
        self.gcs_hook.upload(
            object=self.gcs_key, filename=self.local_key, bucket=self.gcs_bucket
        )

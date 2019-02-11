from tempfile import NamedTemporaryFile
from airflow.models import BaseOperator

from airflow.utils.decorators import apply_defaults

from ibm_wca_plugin.hooks.ibm_wca_hook import IbmWcaHook
from airflow.contrib.hooks.gcs_hook import GoogleCloudStorageHook


class IbmWcaExportListToGCSOperator(BaseOperator):
    template_fields = ("gcs_key", "gcs_bucket")

    @apply_defaults
    def __init__(
        self,
        ibm_wca_conn_id,
        gcs_conn_id,
        list_id,
        gcs_bucket,
        gcs_key,
        *args,
        **kwargs
    ):
        super().__init__(*args, **kwargs)
        self.list_id = list_id
        self.gcs_bucket = gcs_bucket
        self.gcs_key = gcs_key
        self.gcs_hook = GoogleCloudStorageHook()
        self.ibm_hook = IbmWcaHook(ibm_wca_conn_id)

    def execute(self, context):
        job = self.ibm_hook.export_list(list_id=self.list_id)
        self.ibm_hook.poll_job_status(job_id=job["JOB_ID"])
        with NamedTemporaryFile("w") as f:
            self.ibm_hook.ftp_get(job["FILE_PATH"], f.name)
            self.gcs_hook.upload(
                filename=f.name, bucket=self.gcs_bucket, object=self.gcs_key
            )

from tempfile import NamedTemporaryFile
from airflow.models import BaseOperator

from airflow.utils.decorators import apply_defaults

from ibm_wca_plugin.hooks.ibm_wca_hook import IbmWcaHook
from airflow.contrib.hooks.gcs_hook import GoogleCloudStorageHook


class IbmWcaExportTableToGCSOperator(BaseOperator):
    template_fields = ("date_start", "date_end", "gcs_key", "gcs_bucket")

    @apply_defaults
    def __init__(
        self,
        ibm_wca_conn_id,
        gcs_conn_id,
        table_name,
        date_start,
        date_end,
        gcs_bucket,
        gcs_key,
        *args,
        **kwargs
    ):
        super().__init__(*args, **kwargs)
        self.table_name = table_name
        self.date_start = date_start
        self.date_end = date_end
        self.gcs_bucket = gcs_bucket
        self.gcs_key = gcs_key
        self.gcs_hook = GoogleCloudStorageHook(gcs_conn_id)
        self.ibm_hook = IbmWcaHook(ibm_wca_conn_id)

    def execute(self, context):
        gcs_key = self.gcs_key
        job = self.ibm_hook.export_table(
            table_name=self.table_name,
            date_start=self.date_start,
            date_end=self.date_end,
        )
        self.ibm_hook.poll_job_status(job_id=job["JOB_ID"], wait=5)
        with NamedTemporaryFile("w") as f:
            self.ibm_hook.ftp_get(job["FILE_PATH"], f.name)
            self.gcs_hook.upload(
                filename=f.name, bucket=self.gcs_bucket, object=gcs_key
            )

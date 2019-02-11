import io
from airflow.models import BaseOperator

from airflow.utils.decorators import apply_defaults
from ibm_wca_plugin.hooks.ibm_wca_hook import IbmWcaHook
from airflow.contrib.hooks.bigquery_hook import BigQueryHook
from airflow.contrib.hooks.gcs_hook import GoogleCloudStorageHook

from xmljson import badgerfish as bf
from xmljson import parker as pk
from xml.etree.ElementTree import fromstring

from lxml.html import Element, tostring


class IbmWcaImportListFromGCSOperator(BaseOperator):

    template_fields = (
        "ibm_wca_conn_id",
        "dataset_id",
        "table_id",
        "gcs_key",
        "gcs_bucket",
    )
    ui_color = "#82B1FF"

    @apply_defaults
    def __init__(
        self,
        ibm_wca_conn_id,
        dataset_id,
        table_id,
        gcs_bucket,
        gcs_key,
        *args,
        **kwargs
    ):
        super().__init__(*args, **kwargs)
        self.ibm_wca_conn_id = ibm_wca_conn_id
        self.gcs_bucket = gcs_bucket
        self.gcs_key = gcs_key
        self.dataset_id = dataset_id
        self.table_id = table_id
        self.ibm_hook = IbmWcaHook(ibm_wca_conn_id)
        self.bq_hook = BigQueryHook()
        self.gcs_hook = GoogleCloudStorageHook()

    def get_src_fields(self):
        bq_conn = self.bq_hook.get_conn().cursor()
        resp = bq_conn.get_schema(self.dataset_id, self.table_id)
        fields = resp["fields"]
        names = []
        for field in fields:
            names.append(field["name"])
        return names

    def get_ibm_fields(self, list_id):
        self.log.info("list_id {}".format(list_id))
        meta = self.ibm_hook.get_list_meta_data(list_id)
        cols = meta["COLUMNS"]["COLUMN"]
        headers = []
        for col in cols:
            headers.append(col["NAME"])
        return headers

    def create_xml_map_file(self, config, src_fields, dest_fields):
        columns = []

        for idx, name in enumerate(src_fields, start=1):
            if name.upper() in (field.upper() for field in dest_fields):
                name = name.upper() if name == "email" else name
                d = {"INDEX": idx, "INCLUDE": "true", "NAME": name}
                columns.append(d)

        map_file = {
            "LIST_INFO": {
                "ACTION": config["action"],
                "LIST_ID": config["list_id"],
                "LIST_TYPE": config["list_type"],
                "HASHEADERS": "true",
                "FILE_TYPE": 0,
                "LIST_VISIBILITY": 1,
            },
            "MAPPING": {"COLUMN": columns},
            "SYNC_FIELDS": {"SYNC_FIELD": {"NAME": config["sync_field"]}},
        }

        if "contact_list_id" in config:
            map_file["CONTACT_LISTS"] = [{"CONTACT_LIST_ID": config["contact_list_id"]}]

        result = bf.etree(map_file, root=Element("LIST_IMPORT"))
        return tostring(result)

    def execute(self, context):
        config = context["params"]
        upload_filename = context["task_instance_key_str"]
        src_fields = self.get_src_fields()
        dest_fields = self.get_ibm_fields(config["list_id"])
        map_file = self.create_xml_map_file(config, src_fields, dest_fields)
        csv_file = self.gcs_hook.download(bucket=self.gcs_bucket, object=self.gcs_key)
        map_filename = "{}.xml".format(upload_filename)
        source_filename = "{}.csv".format(upload_filename)

        self.ibm_hook.ftp_put("upload/{}".format(map_filename), io.BytesIO(map_file))
        self.ibm_hook.ftp_put("upload/{}".format(source_filename), io.BytesIO(csv_file))
        job = self.ibm_hook.import_list(map_filename, source_filename)
        return self.ibm_hook.poll_job_status(job_id=job["JOB_ID"])


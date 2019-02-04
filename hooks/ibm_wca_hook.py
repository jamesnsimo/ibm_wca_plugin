import time

from json import dumps, loads

from airflow import AirflowException
from airflow.models import Connection
from airflow.hooks.http_hook import HttpHook
from airflow.contrib.hooks.ftp_hook import FTPHook

from airflow.utils.db import provide_session
from airflow.utils.log.logging_mixin import LoggingMixin

from xmljson import badgerfish as bf
from xmljson import parker as pk
from xml.etree.ElementTree import fromstring
from lxml.html import Element, tostring


class IbmWcaHook(HttpHook, LoggingMixin):
    def __init__(self, ibm_wca_conn_id):
        super().__init__(http_conn_id=ibm_wca_conn_id)
        self.ibm_wca_conn_id = ibm_wca_conn_id
        self.conn = self.get_connection(self.http_conn_id)
        self.conn_id = self.conn.conn_id
        self.extra = self.conn.extra_dejson

    # Get access_token for XML API call Authorization header and FTP password
    def _get_token(self, data=None):
        endpoint = "/oauth/token"
        data = {
            "client_id": self.extra.get("client_id"),
            "client_secret": self.extra.get("client_secret"),
            "refresh_token": self.conn.password,
            "grant_type": "refresh_token",
        }
        auth = super().run(endpoint, data)
        return loads(auth.text)["access_token"]

    # Call XML API with method functions below.
    def _xml_api(self, payload, headers=None, extra_options=None):
        endpoint = "/XMLAPI"
        data = tostring(bf.etree({"Body": payload}, root=Element("Envelope")))
        headers = {
            "Content-Type": "text/xml",
            "Authorization": "Bearer " + self._get_token(),
            "Accept": "text/html,application/xhtml+xml,application/xml",
        }
        response = super().run(endpoint, data, headers, extra_options)
        return _parse_response(response.text)

    # FTP Client.
    @provide_session
    def _ftp_client(self, session=None):
        ftp_conn_id = self.conn_id.replace("ibm_wca", "ibm_wca_ftp")
        connection = Connection(
            conn_id=ftp_conn_id,
            conn_type="ftp",
            host="transfer{}.ibmmarketingcloud.com".format(self.extra.get("pod")),
            login="oauth",
            password=self._get_token(),
        )
        session.query(Connection).filter(
            Connection.conn_id == connection.conn_id
        ).delete()
        session.add(connection)
        session.commit()
        return FTPHook(ftp_conn_id)

    # FTP
    def ftp_get(self, remote_path, local_path):
        return self._ftp_client().retrieve_file(remote_path, local_path)

    def ftp_put(self, remote_path, local_path):
        return self._ftp_client().store_file(remote_path, local_path)

    # GetUserProfile
    def load_user_profile(self):
        payload = {"LoadUserProfile": [""]}
        return self._xml_api(payload=payload)

    # GetListMetaData
    def get_list_meta_data(self, list_id):
        payload = {"GetListMetaData": {"LIST_ID": [list_id]}}
        return self._xml_api(payload=payload)

    # ExportList
    def export_list(self, list_id, export_type="ALL", export_format="CSV"):
        payload = {
            "ExportList": {
                "LIST_ID": [list_id],
                "EXPORT_TYPE": [export_type],
                "EXPORT_FORMAT": [export_format],
            }
        }
        return self._xml_api(payload=payload)

    # ExportTable
    def export_table(
        self,
        table_name,
        export_format="CSV",
        table_visibility=1,
        date_start=None,
        date_end=None,
    ):
        payload = {
            "ExportTable": {
                "TABLE_NAME": [table_name],
                "EXPORT_FORMAT": [export_format],
                "TABLE_VISIBILITY": [table_visibility],
                "DATE_START": [date_start],
                "DATE_END": [date_end],
            }
        }
        return self._xml_api(payload=payload)

    # GetJobStatus
    def get_job_status(self, job_id):
        payload = {"GetJobStatus": {"JOB_ID": [job_id]}}
        return self._xml_api(payload=payload)

    # Poll GetJobStatus
    def poll_job_status(self, job_id, wait=10):
        job = self.get_job_status(job_id)
        status = job["JOB_STATUS"]
        self.log.info("IBM WCA Job: %s Status: %s", job_id, status)
        if status in ["RUNNING", "WAITING"]:
            time.sleep(wait)
            return self.poll_job_status(job_id, wait)
        elif status == "COMPLETE":
            return job
        else:
            return AirflowException("IBM WCA Job: %s Status: %s", job_id, status)


# Parse XML to JSON
def _parse_xml(xml):
    return dumps(pk.data(fromstring(xml), preserve_root=False))


# Parse XML API Response, return data or raise AirflowException
def _parse_response(resp):
    b = loads(_parse_xml(resp))["Body"]
    success = b["RESULT"]["SUCCESS"]
    if success not in [True, "SUCCESS"]:
        fault = b["Fault"]["FaultString"]
        raise AirflowException("IBM WCA: {}".format(fault))
    else:
        res = b["RESULT"]
        res.pop("SUCCESS", None)
        return res

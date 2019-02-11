# Airflow Plugin - IBM Watson Campaign Automation

This plugin moves data from the [IBM Watson Campaign Automation XML API](https://developer.ibm.com/customer-engagement/docs/watson-marketing/ibm-engage-2/watson-campaign-automation-platform/xml-api/api-xml-overview/) to Google Cloud Storage based on the specified List or Table ID.

## Dependencies

This plugin requires the following packages

```
lxml
xmljson
```

## Connection

Create a new HTTP Connection with the following parameters

- **Connection ID** naming convention: `ibm_wca_{org nickname}` i.e. `ibm_wca_fdr`.
- **Host**: `api{your Pod number}.ibmmarketingcloud.com` i.e. `api3.ibmmarketingcloud.com`.
- **Password**: `refresh_token` generated by IBM WCA.
- **Extra**: JSON string containing the following properties:

  - `pod`: IBM WCA Pod number
  - `client_id`: IBM WCA client_id
  - `client_secret`: IBM WCA client_secret

  Example: `{ "pod": 3, "client_id": "xxxxxxxxxxxxxxxxx", "client_secret": "xxxxxxxxxxxxxxxxx" }`

## Hooks

### IBM Watson Campaign Automation Hook

Handles the authentication and requests to the XML API as well as FTP server by extending the [HttpHook](https://airflow.apache.org/code.html?highlight=http_hook#airflow.hooks.http_hook.HttpHook) and [FTPHook](https://airflow.apache.org/code.html?highlight=ftp_hook#airflow.contrib.hooks.ftp_hook.FTPHook). This hook allows you to make OAuth authenticated calls to import and export data via XML API and securely interact with FTP server to move the target files to and from Google Cloud Storage. A refresh_token will be generated and stored encrypted as Variable; refreshed after three hours.

### Google Cloud Storage Hook

[Contrib Airflow GoogleCloudStorageHook](https://airflow.readthedocs.io/en/stable/_modules/airflow/contrib/hooks/gcs_hook.html) interacts with Google Cloud Storage. This hook uses the Google Cloud Platform connection.

## Operators

### IbmWcaExportListToGCSOperator

Calls [`ExportList`](https://developer.ibm.com/customer-engagement/tutorials/export-from-a-database/) XML API endpoint to initiate a list export, polls [`GetJobStatus`](https://developer.ibm.com/customer-engagement/tutorials/get-status-data-job/) endpoint until job is complete and transfers the output file from FTP server to Google Cloud Storage.

Accepts the following parameters:

    :param ibm_wca_conn_id:   IBM WCA Connection Id (HTTP Connection)
    :param list_id:           IBM WCA List Id to export
    :param gcs_conn_id:       GCS Connection Id
    :param gcs_bucket:        GCS bucket name (templated)
    :param gcs_key:           GCS destination filename (templated)

#### Example DAG

```python
import datetime
import logging

import airflow
from airflow import models

from airflow.operators import IbmWcaExportListToGCSOperator

default_dag_args = {"start_date": airflow.utils.dates.days_ago(2)}

ORG = "fdr_sb"
LIST_ID = 7797507

with models.DAG(
    "ibm_export_list_to_gcs", schedule_interval=None, default_args=default_dag_args
) as dag:

    export_list_to_gcs = IbmWcaExportListToGCSOperator(
        task_id="export_list",
        ibm_wca_conn_id="ibm_wca_{}".format(ORG),
        list_id=LIST_ID,
        gcs_conn_id="google_cloud_storage_default",
        gcs_bucket="{{ var.value.gcs_bucket_data }}",
        gcs_key="{0}/{1}_{2}.csv".format(ORG, "{{ ds_nodash }}", LIST_ID),
    )

    export_list_to_gcs
```

### IbmWcaExportTableToGCSOperator

Calls [`ExportTable`](https://developer.ibm.com/customer-engagement/tutorials/export-from-a-relational-table/) XML API endpoint to initiate a table export, polls [`GetJobStatus`](https://developer.ibm.com/customer-engagement/tutorials/get-status-data-job/) endpoint until job is complete and transfers the output file from FTP server to Google Cloud Storage.

Accepts the following parameters:

    :param ibm_wca_conn_id:   IBM WCA Connection Id (HTTP Connection)
    :param table_name:        IBM WCA Table to export
    :param date_start:        Specifies the beginning boundary of information to export
                              (relative to the last modified date of the row).
                              If time is included, it must be in 24-hour format. i.e. "12/25/2019 00:00:00" (templated)
    :param date_end:          Specifies the ending boundary of information to export
                              (relative to the last modified date of the row).
                              If time is included, it must be in 24-hour format.. i.e. "12/25/2019 00:00:00" (templated)
    :param gcs_conn_id:       GCS Connection Id
    :param gcs_bucket:        GCS bucket name (templated)
    :param gcs_key:           GCS destination filename (templated)

#### Example DAG

```python
import datetime
import logging

from airflow import models
from airflow import utils

from airflow.operators import IbmWcaExportTableToGCSOperator

default_dag_args = {"start_date": utils.dates.days_ago(2)}

ORG = "fdr_sb"
TABLE_NAME = "SURVEY_CONNECTOR_RESPONSES"

with models.DAG(
    "ibm_export_table_to_gcs", schedule_interval=None, default_args=default_dag_args
) as dag:

    export_table_to_gcs = IbmWcaExportTableToGCSOperator(
        task_id="export_table_to_gcs",
        ibm_wca_conn_id="ibm_wca_fdr_sb",
        gcs_conn_id="google_cloud_storage_default",
        table_name=TABLE_NAME,
        date_start='{{ (previous_execution_date - macros.timedelta(hours=8)).strftime("%m/%d/%Y %H:%M:%S") if previous_execution_date else (execution_date - macros.timedelta(days=10,hours=8)).strftime("%m/%d/%Y %H:%M:%S") }}',
        date_end='{{ (execution_date - macros.timedelta(hours=8)).strftime("%m/%d/%Y %H:%M:%S") }}',
        gcs_bucket="{{ var.value.gcs_bucket_data }}",
        gcs_key="{0}/{1}_{2}.csv".format(ORG, "{{ ds_nodash }}", TABLE_NAME),
    )

    export_table_to_gcs
```

### IbmWcaImportListFromGCSOperator

Generates an XML 'map file' from param.params, BigQuery source table headers and destination database headers via [`GetListMetaData`](https://developer.ibm.com/customer-engagement/tutorials/get-database-details/). Calls [`ImportList`](https://developer.ibm.com/customer-engagement/tutorials/import-to-a-database/) XML API endpoint to initiate a database import, polls [`GetJobStatus`](https://developer.ibm.com/customer-engagement/tutorials/get-status-data-job/) endpoint until job is complete and returns `job` object with various details.

Accepts the following parameters:

    :param ibm_wca_conn_id:   IBM WCA Connection Id (HTTP Connection)
    :param table_name:        IBM WCA Table to export
    :param date_start:        Specifies the beginning boundary of information to export
                              (relative to the last modified date of the row).
                              If time is included, it must be in 24-hour format. i.e. "12/25/2019 00:00:00" (templated)
    :param date_end:          Specifies the ending boundary of information to export
                              (relative to the last modified date of the row).
                              If time is included, it must be in 24-hour format.. i.e. "12/25/2019 00:00:00" (templated)
    :param gcs_conn_id:       GCS Connection Id
    :param gcs_bucket:        GCS bucket name (templated)
    :param gcs_key:           GCS destination filename (templated)

#### Example DAG

```python
import datetime

from airflow import models

from airflow.utils import trigger_rule
from airflow.operators import bash_operator
from airflow.contrib.operators import bigquery_operator
from airflow.contrib.operators import bigquery_to_gcs
from airflow.operators import IbmWcaImportListFromGCSOperator

yesterday = datetime.datetime.combine(
    datetime.datetime.today() - datetime.timedelta(1), datetime.datetime.min.time()
)

ORG = "fdr_sb"
LIST_ID = 7797507
PARAMS = {
    "list_id": 7797507,
    "audience": "fdr_leads",
    "action": "ADD_AND_UPDATE",
    "list_type": 0,
    "sync_field": "EMAIL",
}
GCS_BUCKET = models.Variable.get("gcs_bucket")
DATASET_ID = "ibm_airflow"
TABLE_ID = "import_list_{}".format(PARAMS["audience"])
IMPORT_LIST_TABLE_ID = "{}.{}".format(DATASET_ID,TABLE_ID)
OUTPUT_FILENAME = "import_list_{{ ts_nodash }}.csv"
GCS_FILENAME = "gs://{}/{}/{}".format(GCS_BUCKET, ORG, OUTPUT_FILENAME)


default_dag_args = {
    "start_date": yesterday,
    "email": models.Variable.get("email"),
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 0,
    "retry_delay": datetime.timedelta(minutes=5),
    "project_id": models.Variable.get("gcp_project"),
}

with models.DAG(
    "ibm_import_list", schedule_interval=None, default_args=default_dag_args
) as dag:

    make_bq_table = bigquery_operator.BigQueryOperator(
        task_id="bq_query_to_table",
        bql="sql/{}/{}.sql".format(ORG, PARAMS["audience"]),
        use_legacy_sql=False,
        destination_dataset_table=IMPORT_LIST_TABLE_ID,
    )

    export_list_to_gcs = bigquery_to_gcs.BigQueryToCloudStorageOperator(
        task_id="export_list_to_gcs",
        source_project_dataset_table=IMPORT_LIST_TABLE_ID,
        destination_cloud_storage_uris=[
           GCS_FILENAME
        ],
        export_format="CSV",
    )

    import_list_to_ibm = IbmWcaImportListFromGCSOperator(
        task_id="import_list_to_ibm",
        retries=1,
        ibm_wca_conn_id="ibm_wca_{}".format(ORG),
        params=PARAMS,
        dataset_id=DATASET_NAME,
        table_id="import_list",
        gcs_bucket=GCS_BUCKET,
        gcs_key="{}/{}".format(ORG, OUTPUT_FILENAME),
    )

    delete_gcp_resources = bash_operator.BashOperator(
        task_id="delete_table_and_file",
        bash_command="bq rm -f -t {} && gsutil rm {}".format(IMPORT_LIST_TABLE_ID,GCS_FILENAME),
        trigger_rule=trigger_rule.TriggerRule.ALL_DONE,
    )

    make_bq_table >> export_list_to_gcs >> import_list_to_ibm >> delete_gcp_resources
```

## TODO

- [x] Improve token handling: store local valid requiring less calls to token endpoint.
- Implement more XML API methods including
  - [x] [`ImportList`](https://developer.ibm.com/customer-engagement/tutorials/import-to-a-database/)
  - [ ] [`RawRecipientDataExport`](https://developer.ibm.com/customer-engagement/tutorials/export-raw-contact-events-2/)

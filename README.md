# Airflow Plugin - IBM Watson Campaign Automation

This plugin moves data from the IBM Watson Campaign Automation XML API to S3 based on the specified object

## Additional Packages

This plugin requires the following packages

```
lxml
xmljson
```

## Hooks

### IBM Watson Campaign Automation (IBM WCA) Hook

This hook handles the authentication and requests to the XML API as well as FTP server. This extends the HttpHook and FTPHook allowing you to make OAuth authenticated calls to execute import and export data calls via XML API and securely interact with the FTP server to move the desired files.

## Operators

### IbmWcaExportListToGCSOperator

This operator calls the 'ExportList' XML API endpoint to initiate a list export, polls the 'GetJobStatus' endpoint until the job is complete and transfers the exported file from FTP server to Google Cloud Storage.

It accepts the following parameters:

    :param ibm_wca_conn_id:   IBM Watson Campaign Automation Connection Id (HTTP Connection)
    :param list_id:           The IBM WCA List Id to export.
    :params gcs_conn_id:      Google Cloud Storage Connection Id
    :params gcs_bucket:       Google Cloud Storage bucket name
    :params gcs_key:          Google Cloud Storage destination filename

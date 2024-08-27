"""
CLoud function to create and run dv360 reports

Ready to try, test and deploy
"""
import json
import time
import datetime
from google.cloud import bigquery, storage
from oauth2client.service_account import ServiceAccountCredentials
from googleapiclient.discovery import build
import functions_framework
from helpers import if_tbl_exists, retry_with_backoff, run_query, get_date
from datetime import date, timedelta

PROJECT_ID = "mdlz-na"
CREDENTIALS_PATH = 'credentials/creds.json'
BUCKET_NAME = 'mdlz_taxonomy_adherence'
API_NAME = 'doubleclickbidmanager'
API_VERSION = 'v2'
SCOPES = 'https://www.googleapis.com/auth/doubleclickbidmanager'
QUERY_DATASET_ID = "poc_dv360_reports"
QUERY_TABLE_ID = "reports_queries"
BACKOFF_RETRIES = 5
BACKOFF_TIME = 10
BQ_QUERIES_TABLE = f"{PROJECT_ID}.{QUERY_DATASET_ID}.{QUERY_TABLE_ID}"
VALID_DATA_RANGE = ["CUSTOM_DATES",
                    "CURRENT_DAY",
                    "PREVIOUS_DAY",
                    "WEEK_TO_DATE",
                    "MONTH_TO_DATE",
                    "QUARTER_TO_DATE",
                    "YEAR_TO_DATE",
                    "PREVIOUS_WEEK",
                    "PREVIOUS_MONTH",
                    "PREVIOUS_QUARTER",
                    "PREVIOUS_YEAR",
                    "LAST_7_DAYS",
                    "LAST_30_DAYS",
                    "LAST_90_DAYS",
                    "LAST_365_DAYS",
                    "ALL_TIME",
                    "LAST_14_DAYS",
                    "LAST_60_DAYS"]

REQUEST_BODY = {
    "dataRange": {
        "range": ""
    }
}


@functions_framework.http
def main(request):
    """
    Docstring - WIP
    """
    print("[GCF] EKAM - Init")
    start = time.time()

    # Handle request
    request_json = json.loads(request.data)

    print("[GCF] EKAM - Get request values")
    logs_dataset_id = request_json.get('logs_dataset_id')
    logs_table_id = request_json.get('logs_table_id')
    report_name_to_run = request_json.get('report')
    data_range = request_json.get('data_range')

    if logs_dataset_id is None or logs_table_id is None or report_name_to_run is None or data_range is None:
        return print("[GCF] EKAM - Missing required parameters. Quitting"
                     )

    if data_range not in VALID_DATA_RANGE:
        return print("[GCF] EKAM - Data range input is not correct. Quitting"
                     )
    REQUEST_BODY['dataRange']['range'] = data_range

    if data_range == 'CUSTOM_DATES':
        REQUEST_BODY['dataRange']['range'] = data_range
        if not request_json.get('customStartDate') or not request_json.get('customEndDate'):
            return print("Data range input is not correct. Should specify start and end Date. Quitting"
                         )
        customStartDate = datetime.datetime.strptime(
            request_json.get(
                'customStartDate'), "%Y%m%d").strftime("%Y-%-m-%-d")
        customEndDate = datetime.datetime.strptime(
            request_json.get(
                'customEndDate'), "%Y%m%d").strftime("%Y-%-m-%-d")
        REQUEST_BODY['dataRange']['customStartDate'] = {}
        REQUEST_BODY['dataRange']['customStartDate']['year'] = customStartDate.split(
            '-')[0]
        REQUEST_BODY['dataRange']['customStartDate']['month'] = customStartDate.split(
            '-')[1]
        REQUEST_BODY['dataRange']['customStartDate']['day'] = customStartDate.split(
            '-')[2]
        REQUEST_BODY['dataRange']['customEndDate'] = {}
        REQUEST_BODY['dataRange']['customEndDate']['year'] = customEndDate.split(
            '-')[0]
        REQUEST_BODY['dataRange']['customEndDate']['month'] = customEndDate.split(
            '-')[1]
        REQUEST_BODY['dataRange']['customEndDate']['day'] = customEndDate.split(
            '-')[2]
    bq_logs_table = f"{PROJECT_ID}.{logs_dataset_id}.{logs_table_id}"

    print("[GCF] EKAM - BigQuery and Storage connection")

    bq_client = bigquery.Client()
    storage_client = storage.Client()
    bucket = storage_client.get_bucket(BUCKET_NAME)

    # Make connection with DV360
    # Get credentials from bucket currently says bucket in service
    print("[GCF] EKAM - DV360 Connection")

    credentials_blob = bucket.blob(CREDENTIALS_PATH)
    with open('/tmp/credentials.json', 'wb') as file_obj:
        credentials_blob.download_to_file(file_obj)
    service_account_credentials = ServiceAccountCredentials.from_json_keyfile_name(
        "/tmp/credentials.json", scopes=SCOPES
    )

    # Build the service object to connect to DV360 API
    print("[GCF] EKAM - Build DV360 API's service")

    service = build(
        API_NAME,
        API_VERSION,
        cache_discovery=False,
        credentials=service_account_credentials
    )

    # Check if the BQ table for storing logs already exists, if not, create it
    print("[GCF] EKAM - Check if table already exists")

    logs_table = bigquery.Table(bq_logs_table)
    if not if_tbl_exists(bq_client, logs_table):
        # Create BQ table
        schema = [
            bigquery.SchemaField("requested_date", "DATE"),
            bigquery.SchemaField("title", "STRING"),
            bigquery.SchemaField("partner_id", "STRING"),
            bigquery.SchemaField("query_id", "STRING"),
            bigquery.SchemaField("report_id", "STRING"),
            bigquery.SchemaField("report_status", "STRING"),
            bigquery.SchemaField("log_id", "STRING")
        ]
        logs_table = bigquery.Table(bq_logs_table, schema=schema)
        # Make an API request to create table
        logs_table = bq_client.create_table(logs_table)
        logs_table = bigquery.Table(bq_logs_table)  # Get table

    # Get logs table and convert into df
    print("[GCF] EKAM - Get logs table")

    logs_df = bq_client.list_rows(logs_table).to_dataframe()

    # Get queries table and convert into df
    print("[GCF] EKAM - Get queries table")

    queries_table = bigquery.Table(BQ_QUERIES_TABLE)  # Get table
    queries_df = bq_client.list_rows(queries_table).to_dataframe()
    queries_df = queries_df[queries_df['report_name'] == report_name_to_run]

    # Handle the date for the report
    user_date = request.args.get('date')
    report_date = get_date(user_date)
    report_date_string = report_date.strftime("%Y-%-m-%-d")
    # Iterate for every query and create the reports
    for index, row in queries_df.iterrows():

        print(index)

        # Check on CF time and retry!
        timeout = round(time.time() - start)
        print(timeout)
        if timeout > 500:
            print('Close to reach timeout. Finishing function and retrying')
            raise RuntimeError('Function crashed')

        # Check if row already exists
        if (logs_df['log_id'] == row['partner_id'] + '_' + report_date_string).any():
            # Skip to next report
            print(
                f'Report named {row["title"]} with start date {report_date} already exists. Skipping to next report')
            continue

        # Run Query with exponential backoff
        report_response = retry_with_backoff(
            lambda query=row['query_id']: run_query(
                service, query, REQUEST_BODY),
            retries=BACKOFF_RETRIES,
            backoff_in_seconds=BACKOFF_TIME
        )

        # insert new row
        print("[GCF] EKAM - Insert new row")

        rows_to_insert = {
            'requested_date': datetime.datetime.today().strftime('%Y-%m-%d'),
            'title': row['title'],
            'partner_id': row['partner_id'],
            'query_id': row['query_id'],
            'report_id': report_response['key']['reportId'],
            'report_status': report_response['metadata']['status']['state'],
            'log_id': row['partner_id'] + '_' + report_date_string
        }

        errors = bq_client.insert_rows_json(bq_logs_table, [rows_to_insert])
        if not errors:
            print("[GCF] EKAM - New rows have been added to logs table.")
        else:
            print(
                f"[GCF] EKAM - Encountered errors while inserting rows: {errors}")
    return "Success!"

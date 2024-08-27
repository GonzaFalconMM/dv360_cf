"""
Docstring
"""
import csv
import json
import time
import requests
import pandas as pd
import pandas_gbq
from google.cloud import bigquery, storage
from oauth2client.service_account import ServiceAccountCredentials
from googleapiclient.discovery import build
import functions_framework
from helpers import if_tbl_exists

PROJECT_ID = "mdlz-na"
BQ_LOGS_DATASET = "poc_dv360_reports"
BUCKET_NAME = "mdlz_dv360_reports"
CREDS_BUCKET_NAME = 'mdlz_taxonomy_adherence'
CREDS_PATH = 'credentials/creds.json'
API_NAME = 'doubleclickbidmanager'
API_VERSION = 'v2'
SCOPES = 'https://www.googleapis.com/auth/doubleclickbidmanager'


@functions_framework.http
def main(request):
    """
    Target function
    """
    print("[GCF] EKAM - Init")

    start = time.time()

    # Handle request
    print("[GCF] EKAM - Handle request")

    request_json = json.loads(request.data)
    bq_logs_table = request_json.get('bq_logs_table')
    bq_report_destination_table = request_json.get(
        'bq_report_destination_table')
    bq_report_destination_dataset = request_json.get(
        'bq_report_destination_dataset')
    bq_finished_table = request_json.get('bq_finished_table')
    destination_folder = request_json.get('destination_folder')

    if (
        bq_logs_table is None or
        bq_report_destination_table is None or
        bq_report_destination_dataset is None or
        bq_finished_table is None or
        destination_folder is None
    ):
        return print("[GCF] EKAM - Missing required parameters. Quitting")

    print("[GCF] EKAM - BigQuery and GCS access")
    # Authenticate the user to access BigQuery and GCS
    bq_client = bigquery.Client()
    storage_client = storage.Client()
    creds_bucket = storage_client.get_bucket(CREDS_BUCKET_NAME)
    bucket = storage_client.get_bucket(BUCKET_NAME)

    # Make connection with DV360
    print("[GCF] EKAM - Make connection with DV360")

    # Get credentials from bucket currently says bucket in service
    credentials_blob = creds_bucket.blob(CREDS_PATH)
    with open('/tmp/credentials.json', 'wb') as file_obj:
        credentials_blob.download_to_file(file_obj)
    service_account_credentials = ServiceAccountCredentials.from_json_keyfile_name(
        "/tmp/credentials.json",
        scopes=SCOPES
    )

    # Build the service object to connect to DV360 API
    service = build(
        API_NAME,
        API_VERSION,
        cache_discovery=False,
        credentials=service_account_credentials
    )

    # Create/Get finished table and convert into df
    finished_table = bigquery.Table(
        f'{PROJECT_ID}.{BQ_LOGS_DATASET}.{bq_finished_table}')

    if not if_tbl_exists(bq_client, finished_table):
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
        finished_table = bigquery.Table(
            f'{PROJECT_ID}.{BQ_LOGS_DATASET}.{bq_finished_table}',
            schema=schema
        )
        # Make an API request to create table
        finished_table = bq_client.create_table(finished_table)
        finished_table = bigquery.Table(
            f'{PROJECT_ID}.{BQ_LOGS_DATASET}.{bq_finished_table}'
        )  # Get table

    # Get finished table and convert into df
    print("[GCF] EKAM - Get finished table")

    finished_df = bq_client.list_rows(finished_table).to_dataframe()

    # Get logs table and convert into df
    print("[GCF] EKAM - Get logs table")

    table = bigquery.Table(f"{PROJECT_ID}.{BQ_LOGS_DATASET}.{bq_logs_table}")
    logs_df = bq_client.list_rows(table).to_dataframe()
    for index, row in logs_df.iterrows():

        # Check on CF time and retry!
        timeout = round(time.time() - start)

        # Check if the report has been already ingested
        if (finished_df['log_id'] == row['log_id']).any():
            continue

        print(timeout)
        if timeout > 450:
            print('Close to reach timeout. Finishing function and retrying')
            raise RuntimeError('Function crashed')

        # Check if the report is ready. If not, skip...
        try:
            # pylint: disable=maybe-no-member
            get_report_response = service.queries().reports().get(
                queryId=row['query_id'],
                reportId=row['report_id']
            ).execute()
            report_url = get_report_response['metadata']['googleCloudStoragePath']
            print('[GCF] EKAM - Report url exists!')
        except Exception as error:
            print(error)
            print(
                f'[GCF] EKAM - Report {row["title"]} still not ready. Skipping...')
            continue

        try:

            # Clear local file just in case...
            local_filename = '/tmp/dv360_report.csv'
            open(local_filename, 'w', encoding="utf-8").close()

            # Download report to a local csv file
            print('[GCF] EKAM - Downloading to csv')
            resp = requests.get(report_url, stream=True)
            with open(local_filename, "wb") as f:
                for chunk in resp.iter_content(chunk_size=1024):
                    f.write(chunk)

            # Find last row
            print('[GCF] EKAM - Finding last row')
            with open(local_filename, "rt", encoding="utf-8") as csvfile:
                reader = csv.reader(csvfile, delimiter=',')
                _n = 0
                for row_f in reader:
                    _n += 1
                    if len(row_f) > 1:
                        if row_f[0] == 'Report Time:':
                            # return n
                            break

            # Create df from csv
            print('[GCF] EKAM - Create df')
            report_df = pd.read_csv(local_filename, sep=',', nrows=_n-4)
            report_df.columns = report_df.columns.str.replace(' ', '_').str.replace(
                ':', '_').str.replace('(', '_').str.replace(')', '_').str.replace('/', '_').str.replace('-', '_')
            if "Partner_ID" in report_df:
                report_df['Partner_ID'] = report_df['Partner_ID'].astype(str)
            if "CM360_Placement_ID" in report_df:
                report_df['CM360_Placement_ID'] = report_df['CM360_Placement_ID'].astype(
                    float)
            report_df.rename(
                columns={"TrueView__Views": "YouTube__Views"}, inplace=True)

            # Upload to GCS
            print('[GCF] EKAM - Uploading to GCS')
            filename = f'{destination_folder}/{row["title"]}.csv'
            blob = bucket.blob(filename)
            blob.upload_from_string(
                report_df.to_csv(), 'text/csv', timeout=120)

            # Upload to BQ
            print('[GCF] EKAM - Upload to BQ')
            if not report_df.empty:
                pandas_gbq.to_gbq(
                    report_df,
                    f"{bq_report_destination_dataset}.{bq_report_destination_table}",
                    project_id=PROJECT_ID,
                    if_exists='append'
                )

            # Add row to completed table
            rows_to_insert = {
                'requested_date': str(row['requested_date']),
                'title': row['title'],
                'partner_id': row['partner_id'],
                'query_id': row['query_id'],
                'report_id': row['report_id'],
                'report_status': row['report_status'],
                'log_id': row['log_id']
            }
            errors = bq_client.insert_rows_json(
                f'{PROJECT_ID}.{BQ_LOGS_DATASET}.{bq_finished_table}',
                [rows_to_insert]
            )
            if not errors:
                print("[GCF] EKAM - New rows have been added to finished table.")
            else:
                print(
                    f"[GCF] EKAM - Encountered errors while inserting rows: {errors}")

        except Exception as error:
            print(
                f'[GCF] EKAM - An error ocurred when downloading/uploading report {row["title"]}')
            print(error)
            continue
    return 'Success!'

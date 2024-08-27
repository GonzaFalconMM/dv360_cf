"""
Helpers functions
"""
import random
import time
from datetime import date, timedelta, datetime
from google.cloud.exceptions import NotFound

def if_tbl_exists(client, table_ref):
    """
    Check if a table in BQ exists or not and return True or False
    """
    try:
        client.get_table(table_ref)
        return True
    except NotFound:
        return False

def run_query(serv, query_id, req_body):
    """
    Runs a query in dv360 to generate a report id
    """
    run_query_response = serv.queries().run(queryId=query_id, body=req_body).execute()
    return run_query_response

def retry_with_backoff(func, retries = 5, backoff_in_seconds = 10):
    """
    Retries a function after certain time if it crashes
    """
    _x = 0
    while True:
        try:
            return func()
        except Exception as error:
            print(error)
            if _x == retries-1:
                return 'Max number of attempts reached. Quitting'
            print(f'Attempt {_x+1} failed. Retrying...')
            sleep = (backoff_in_seconds * 2 ** _x + random.uniform(0, 1))
            time.sleep(sleep)
            _x += 1

def get_date(usr_date):
    """
    Returns the formated date string for the report to be run
    """
    if usr_date is not None:
        try:
            report_day = datetime.date(datetime.strptime(str(usr_date), "%Y%m%d"))
        except Exception as error:
            print(error)
            raise RuntimeError('The date format provided is not valid. Quitting...') from error
    else:
        report_day = date.today() - timedelta(1)
    return report_day

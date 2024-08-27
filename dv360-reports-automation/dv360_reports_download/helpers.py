"""
Helper functions for main
"""
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

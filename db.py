import constants
import pandas as pd
import datetime
import numpy as np

def get_between(site_code, since1, since2):
    if since1 == since2:
        return
    selection = 'SELECT * from {} WHERE SITE_CODE = \'' + site_code + '\' AND TIME BETWEEN \'' + since1 + '\' AND \'' + since2 + '\' ORDER BY TIME';
    print(selection.format(constants.data_table))
    stored_df = pd.read_sql(
        selection.format(constants.data_table), constants.postgres_engine
    )
    return stored_df

def get_some(fraction):
    selection = 'SELECT * FROM {} TABLESAMPLE BERNOULLI(' + str(fraction) + ');'
    print(selection.format(constants.data_table))
    stored_df = pd.read_sql(
        selection.format(constants.data_table), constants.postgres_engine
    )
    return stored_df

def count():
    selection = 'SELECT COUNT(*) FROM {};'
    print(selection.format(constants.data_table))
    stored_df = pd.read_sql(
        selection.format(constants.data_table), constants.postgres_engine
    )
    return stored_df

def version():
    v = pd.read_sql("SELECT VERSION()", constants.postgres_engine)
    print(v)

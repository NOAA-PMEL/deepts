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

def get_by_stride(site_code, total_est, start_date, end_date):
    millis1 = start_date*1000000000
    millis2 =   end_date*1000000000
    selection = 'select s.* from (select t.*,row_number() over(order by t.millis) as rnk from {} t where t.site_code="{}" AND t.millis between {} and {}) s where mod(s.rnk,( {}/20000)) = 0'
    print(selection.format(constants.data_table, site_code, millis1, millis2, round(total_est)))
    stored_df = pd.read_sql(
        selection.format(constants.data_table, site_code, millis1, millis2, round(total_est)), constants.postgres_engine
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


def get_locations():
    selection = "SELECT * FROM {}"
    selection = selection.format(constants.location_table)
    locations_df = pd.read_sql(selection, constants.postgres_engine)
    return locations_df

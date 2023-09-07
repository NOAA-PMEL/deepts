import os
import redis
import pandas as pd
import numpy as np
import json

import constants
import ssl
import datetime 

import logging
from sdig.erddap.info import Info

logging.basicConfig(level=logging.DEBUG)


def load_locations():
    locations_df = None
    with open("config/sites.json", "r+") as site_file:
        site_json = json.load(site_file)
    for site in site_json:
        site_df = pd.read_csv(site_json[site]['locations'], skiprows=[1])
        if site_df.shape[1] > 1:
            site_df = site_df.mean().round(2)
            site_df = site_df.reset_index().pivot_table(columns='index')
            site_df['site_code'] = site
        if locations_df is None:
            locations_df = site_df
        else:
            locations_df = pd.concat([locations_df, site_df])
    locations_df = locations_df.reset_index()
    locations_df.to_sql(constants.location_table, constants.postgres_engine, if_exists='replace', index=False, chunksize=1500, method='multi')

def load_observations():
    with open("config/sites.json", "r+") as site_file:
        site_json = json.load(site_file)
    for isite, site in enumerate(site_json):
        url = site_json[site]['url']

        info = Info(url)
        variables, long_names, units, standard_names, var_types = info.get_variables()
        start_date, end_date, start_date_seconds, end_date_seconds = info.get_times()

        start_dt = datetime.datetime.fromtimestamp(start_date_seconds)
        end_dt = datetime.datetime.fromtimestamp(end_date_seconds)
        first_dt = start_dt
        next_dt = start_dt + datetime.timedelta(days=365)

        years = 0
        while next_dt < end_dt + datetime.timedelta(days=365):

            if years == 0:
                time_con = '&time>='+first_dt.isoformat()+'&time<='+next_dt.isoformat()
            else:
                time_con = '&time>'+first_dt.isoformat()+'&time<='+next_dt.isoformat()
            url = site_json[site]['url'] + '.csv?' + ','.join(variables) + time_con
            logging.info('Reading data from ' + url)
            dtypes = {}
            for var in var_types:
                if var_types[var] == 'String':
                    dtypes[var] = 'str'
                elif (var_types[var] == 'float' or var_types[var] == 'double') and var != 'time':
                    dtypes[var] = np.float64
                else:
                    logging.warn(var + ' has unkown type of ' + var_types[var])

            df = pd.read_csv(url, skiprows=[1], dtype=dtypes, parse_dates=True)

            df = df.dropna(subset=['latitude','longitude'], how='any')
            df = df.query('-90.0 <= latitude <= 90')
            df = df.sort_values('time')
            df.reset_index(drop=True, inplace=True)
            df.loc[:,'millis'] = pd.to_datetime(df['time']).view(np.int64)
            df.loc[:,'text_time'] = df['time'].astype(str)

            # logging.info('Preparing sub-sets for locations and counts.')
            # locations_df = df.groupby('platform_code', as_index=False).last()

            # counts_df = df.groupby('platform_code').count()
            # counts_df.reset_index(inplace=True)

            logging.info('Found ' + str(df.shape[0]) + ' observations to store.')

            # In the following command, we are saving the updated new data to the dataset_table using pandas
            # and the SQLAlchemy engine we created above. When if_exists='append' we add the rows to our table
            # and when if_exists='replace', a new table overwrites the old one.
            logging.info('Updating data...')
            if isite == 0:
                df.to_sql(constants.data_table, constants.postgres_engine, if_exists='replace', index=False, chunksize=1500, method='multi')
            else:
                df.to_sql(constants.data_table, constants.postgres_engine, if_exists='append', index=False, chunksize=1500, method='multi')            
            # logging.info('Updating counts...')
            # counts_df.to_sql(constants.counts_table, constants.postgres_engine, if_exists='replace', index=False)
            # logging.info('Updating locations...')
            # locations_df.to_sql(constants.locations_table, constants.postgres_engine, if_exists='replace', index=False)
            first_dt = next_dt
            next_dt = next_dt + datetime.timedelta(days=365)
            years = years + 1


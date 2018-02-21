import pandas as pd
import numpy as np
import mysql.connector
import os
import copy
import utm
try:
    from weather_forecast_retrieval import hrrr
except:
    pass

import matplotlib.pyplot as plt
import scipy.spatial

sql_user={'user': 'micahsandusky',
          'password': 'B3rj3r+572',
          'host': '10.200.28.137',
          'database': 'weather_db',
          'port': '32768'
          }

description = 'HRRR data points nearest to met. stations'
chunk_size = 75000

class NumpyMySQLConverter(mysql.connector.conversion.MySQLConverter):
    """ A mysql.connector Converter that handles Numpy types """

    def _float32_to_mysql(self, value):
        return float(value)

    def _float64_to_mysql(self, value):
        return float(value)

    def _int32_to_mysql(self, value):
        return int(value)

    def _int64_to_mysql(self, value):
        return int(value)

def chunks(l, n):
    """Yield successive n-sized chunks from l."""
    for i in range(0, len(l), n):
        yield l[i:i + n]

def get_station_meta(client):
    cnx = mysql.connector.connect(user=sql_user['user'],
                                  password=sql_user['password'],
                                  host=sql_user['host'],
                                  database=sql_user['database'],
                                  port=sql_user['port'])

    # get the station id's
    qry = ("SELECT tbl_metadata.* FROM tbl_metadata INNER JOIN"
    " tbl_stations_view ON"
    " tbl_metadata.primary_id=tbl_stations_view.primary_id"
    " WHERE client='{}'".format(client))

    print(qry)
    print('')

    d = pd.read_sql(qry, cnx, index_col='primary_id')

    # select the data from tbl_level2
    sta = "','".join(d.index)

    # save metadata
    d_meta = d.copy()
    # check to see if UTM locations are calculated
    d_meta['X'] = d_meta['utm_x']
    d_meta['Y'] = d_meta['utm_y']

    cnx.close()

    return d_meta

def put_hrrr_station(df, description):
    cnx = mysql.connector.connect(user=sql_user['user'],
                                  password=sql_user['password'],
                                  host=sql_user['host'],
                                  database=sql_user['database'],
                                  port=sql_user['port'])

    cnx.set_converter_class(NumpyMySQLConverter)

    table = ['tbl_hrrr_compare']

    for tbl in table:
        print('Adding/updating {} ({} values) to the database table {}'.format(
            description, len(df), tbl))

        try:
            # replace Null with None
            df = df.where((pd.notnull(df)), None)

            # create a bulk insert for the data
            wildcards = ','.join(['%s'] * len(df.columns))
            colnames = ','.join(df.columns)
            update = ','.join(['{}=VALUES({})'.format(c,c) for c in df.columns])
            insert_sql = 'INSERT INTO {0} ({1}) VALUES ({2}) ON DUPLICATE KEY UPDATE {3}'.format(
                tbl, colnames, wildcards, update)

            data = [tuple(rw) for rw in df.values]
            cur = cnx.cursor()

            for d in chunks(data, chunk_size):
                cur.executemany(insert_sql, d)
                cnx.commit()

        except mysql.connector.Error as err:
                print(err)

    cnx.close()

def collect_station_precip():
    """
    Get the precip at pixels near met. stations from the grib files. Put
    this data into the SQL database.
    Args:
    Returns:
    """
    ### This should be config stuff ###

    # get dates (will read in full day)
    start_date = pd.to_datetime('2018-02-19 01:00')
    end_date = pd.to_datetime('2018-02-19 12:00')
    client = 'TUOL_2017'
    var_keys = ['precip_int']
    dx = 3000  # m
    dy = 3000  # m
    # max distance through a cell
    dxy = np.sqrt(dx*dx + dy*dy)

    print(start_date, end_date)

    ### start real procedure ###

    # get station meta data
    df_meta = get_station_meta(client)
    utm_x = df_meta['utm_x'].values
    utm_y = df_meta['utm_y'].values
    sid = df_meta.index.values
    utm_zone = df_meta['utm_zone'].values.astype(int)
    id_max = np.where(utm_x == np.max(utm_x))[0][0]
    id_min = np.where(utm_x == np.min(utm_x))[0][0]

    ur = np.array(utm.to_latlon(np.max(utm_x), np.max(utm_y), utm_zone[id_max], 'N'))
    ll = np.array(utm.to_latlon(np.min(utm_x), np.min(utm_y), utm_zone[id_min], 'N'))

    buff = 0.1 # buffer of bounding box in degrees
    ur += buff
    ll -= buff
    bbox = np.append(np.flipud(ll), np.flipud(ur))

    # make new df
    df_meta_new = df_meta.filter(items=['primary_id'])

    # read grib files for keys
    metadata, data = hrrr.HRRR(configFile='../scripts/hrrr.ini').get_saved_data(
                                            start_date,
                                            end_date,
                                            bbox,
                                            output_dir='/data/snowpack/forecasts/hrrr',
                                            force_zone_number=utm_zone[id_max],
                                            var_keys=var_keys)

    # find idx, idy based on first grib timestep
    hrrr_code = []
    min_dist_lst = []

    for ii, (x,y,st_id) in enumerate(zip(utm_x,utm_y,sid)):
    	# Find index
    	ady =  np.abs(metadata['utm_y'].values.astype(float)-y)
    	#yind = np.where(ady == np.min(ady))#[0]
    	adx =  np.abs(metadata['utm_x'].values.astype(float)-x)
    	#xind = np.where(adx == np.min(adx))#[0]
        adxy = np.sqrt(ady**2 + adx**2)
        min_dist = np.min(adxy)
        min_ind = np.where(adxy == min_dist)[0][0]

        hrrr_code.append(metadata.index.values[min_ind])
        min_dist_lst.append(min_dist)
        if min_dist > dxy:
            raise ValueError('distance to grid is too large: {}'.format(min_dist))

    # store corresponding id's
    df_meta_new['hrrr_code'] = hrrr_code
    df_meta_new['dist_from_grid'] = min_dist_lst
    #print(df_meta_new)

    # create dataframe to store precip data
    df_new = pd.DataFrame(columns=['date_time', 'station_id'])
    # lists for extracting data
    prim_id = []
    date_time_final = []
    ppt_final = []
    hrrr_time = data['precip_int'].index.values

    # loop through stations to put into new dataframe
    for ht, st in zip(df_meta_new['hrrr_code'].values, df_meta_new.index.values):
        # loop through time indeces
        for idt, t in enumerate(hrrr_time):
            # format time for database
            tfmt = pd.to_datetime(t).strftime('%Y-%m-%d %H:%M')
            date_time_final.append(tfmt)
            # get precip at time
            ppt_final.append(float(data['precip_int'][ht][t]))
            prim_id.append(st)

    # store data in datafram that will be sent to sql database
    df_new['date_time'] = date_time_final
    df_new['station_id'] = prim_id
    df_new['precip_accum'] = ppt_final
    #df_new.set_index('date_time', inplace=True)
    print(df_new)

    # put df on database
    put_hrrr_station(df_new, description)

    print('\n\n-----Done-----\n\n')


if __name__ == '__main__':
    collect_station_precip()

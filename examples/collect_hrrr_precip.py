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
    dx = 3000  # m
    dy = 3000  # m
    # max distance through a cell
    dxy = np.sqrt(dx*dx + dy*dy)

    print(start_date, end_date)

    ### start real procedure ###

    # get station meta data
    df_meta = get_station_meta(client)
    print(df_meta)
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

    print(bbox)

    # make new df
    df_new = df_meta.filter(items=['primary_id'])

    # read grib files for keys
    metadata, data = hrrr.HRRR(configFile='../scripts/hrrr.ini').get_saved_data(
                                            start_date,
                                            end_date,
                                            bbox,
                                            output_dir='/data/snowpack/forecasts/hrrr',
                                            force_zone_number=utm_zone[id_max],
                                            var_keys=['precip_int'])

    # find idx, idy based on first grib timestep
    print(metadata)
    #print(data)
    hrrr_code = []
    min_dist_lst = []

    for ii, (x,y,st_id) in enumerate(zip(utm_x,utm_y,sid)):
    	# Find index
        # pt = np.array((x,y))
        # print(pt)
        # dist = scipy.spatial.distance.cdist(hrrr_pts, pt)
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

    df_new['hrrr_code'] = hrrr_code
    df_new['dist_from_grid'] = min_dist_lst
    print(df_new)
    # make df for stations for every hour returned for grib

    # for t_hr in hrrr data
        # put ppt in df for each station

    # put df on database

if __name__ == '__main__':
    collect_station_precip()

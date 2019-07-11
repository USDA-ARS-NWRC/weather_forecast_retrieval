#!/usr/bin/env python

from weather_forecast_retrieval import hrrr, utils
from weather_forecast_retrieval.grib2nc import grib2nc
import os
import argparse
import pandas as pd
from datetime import datetime
import logging

def run(config_file, start_date):

    if os.path.isfile(config_file):
        config = utils.read_config(config_file)

        # parse the rest of the config file
        output_dir = config['output']['output_dir']
        output_nc = config['output']['output_nc']

        # get the date
        ex_date = datetime.date(pd.to_datetime(start_date))
        date_folder = 'hrrr.{}'.format(ex_date.strftime('%Y%m%d'))

        # setup logging
        logfile = None
        if 'grib2nc_log' in config['logging']:
            logfile = config['logging']['grib2nc_log']

        fmt = '%(levelname)s:%(name)s:%(message)s'
        log = logging.getLogger('grib2nc')
        numeric_level = 1
        if logfile is not None:
            handler = TimedRotatingFileHandler(logfile,
                                                when='D',
                                                interval=1,
                                                utc=True,
                                                atTime=time(),
                                                backupCount=30)
            log.setLevel(numeric_level)
            formatter = logging.Formatter(fmt)
            handler.setFormatter(formatter)
            log.addHandler(handler)
        else:
            logging.basicConfig(level=numeric_level)

        # create the hrrr grib2 output path
        grib2_path = os.path.join(output_dir, date_folder)
        nc_path = os.path.join(output_nc, date_folder)
        if not os.path.isdir(nc_path):
            os.mkdir(nc_path)

        # look for all files in a directory
        files = os.listdir(grib2_path)
        for f in files:
            grib_file = os.path.join(grib2_path, f)
            nc_file = os.path.join(nc_path, ".".join(f.split(".")[0:-1]) + ".nc")
            grib2nc(grib_file, nc_file, log)

    else:
        raise IOError('Config file does not exist.')


if __name__ == '__main__':

    parser = argparse.ArgumentParser(description='Convert folder of HRRR grib2 files to netcdf')
    parser.add_argument('config_file', metavar='config_file', type=str,
                        help='Path to config file')
    
    parser.add_argument('-s', '--start', dest='start_date',
                        required=True, default=None,
                        help='Date to convert')

    args = parser.parse_args()

    run(args.config_file, args.start_date.strip())

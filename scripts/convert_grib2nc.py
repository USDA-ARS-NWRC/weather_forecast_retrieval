#!/usr/bin/env python

from weather_forecast_retrieval import hrrr
import os
import argparse

def run():

    parser = argparse.ArgumentParser(description='Run the data retrieval for HRRR model')
    parser.add_argument('config_file', metavar='config_file', type=str,
                        help='Path to config file')
    
    parser.add_argument('-s', '--start', dest='start_date',
                        required=False, default=None,
                        help='Start date to look for modified files')

    parser.add_argument('-e', '--end', dest='end_date',
                        required=False, default=None,
                        help='End date to look for modified files')

    args = parser.parse_args()

    if os.path.isfile(args.config_file):
        hrrr.HRRR(args.config_file).retrieve_http_by_date(args.start_date, args.end_date)

    else:
        raise IOError('File does not exist.')


if __name__ == '__main__':
    run()
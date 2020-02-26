"""
Code retrieved from Brian Baylock at University of Utah.
This code allows for downloading of historical HRRR data.

Code modified by Micah Sandusky at USDA ARS NWRC.

Documentation from the code:
Download archived HRRR files from MesoWest Pando S3 archive system.

Please register before downloading from our HRRR archive:
http://hrrr.chpc.utah.edu/hrrr_download_register.html

For info on the University of Utah HRRR archive and to see what dates are
available, look here:
http://hrrr.chpc.utah.edu/

"""

import argparse
import os
import time
from datetime import datetime

import pandas as pd
import pytz
import requests

from weather_forecast_retrieval import utils

# times when downloading should stop as recomended by U of U
tzmdt = pytz.timezone('America/Denver')
no_hours = [0, 3, 6, 9, 12, 15, 18, 21]


def check_before_download(logger):
    """
    See if it is an okay time to download from U of U based on the times
    that they are pulling data.
    """
    nowutc = datetime.utcfromtimestamp(time.time())
    tzmdt = pytz.timezone('America/Denver')
    nowtime_mdt = nowutc.replace(tzinfo=pytz.utc).astimezone(tzmdt)

    this_hour = nowtime_mdt.time().hour
    this_min = nowtime_mdt.time().minute

    # make sure we are not initiating download at an inconvenient time
    if this_hour in no_hours:
        while this_hour in no_hours and this_min > 30:
            # nowtime = datetime.now()
            nowutc = datetime.utcfromtimestamp(time.time())
            nowtime_mdt = nowutc.replace(tzinfo=pytz.utc).astimezone(tzmdt)
            # print(nowtime_mdt)
            logger.info('Sleeping {}'.format(nowtime_mdt))
            this_hour = nowtime_mdt.time().hour
            this_min = nowtime_mdt.time().minute

            time.sleep(100)


def download_url(fname, OUTDIR, logger, file_day, model='hrrr', field='sfc'):
    """
    Construct full URL and download file

    Args:
        fname:      HRRR file name
        OUTDIR:     Location to put HRRR file
        logger:     Logger instance
        file_day:   datetime date correspondig to the hrrr
                    file (i.e hrrr.{date}/hrrr...)

    Returns:
        success:    boolean of weather or not we were succesful

    """

    URL = "https://pando-rgw01.chpc.utah.edu/%s/%s/%s/%s" \
        % (model, field, file_day.strftime('%Y%m%d'), fname)

    # 2) Rename file with date preceeding original filename
    #    i.e. hrrr.20170105/hrrr.t00z.wrfsfcf00.grib2
    rename = "hrrr.%s/%s" \
             % (file_day.strftime('%Y%m%d'), fname)

    # create directory if not there
    redir = os.path.join(OUTDIR, 'hrrr.%s' % (file_day.strftime('%Y%m%d')))
    if not os.path.exists(redir):
        os.makedirs(redir)

    # 3) Download the file via https
    # Check the file size, make it's big enough to exist.
    check_this = requests.head(URL)
    file_size = int(check_this.headers['content-length'])

    try:
        if file_size > 10000:
            logger.info("Downloading: {}".format(URL))
            new_file = os.path.join(OUTDIR, rename)
            r = requests.get(URL, allow_redirects=True)
            with open(new_file, 'wb') as f:
                f.write(r.content)
                logger.debug('Saved file to: {}'.format(new_file))
            success = True
        else:
            # URL returns an "Key does not exist" message
            logger.error("ERROR:", URL, "Does Not Exist")
            success = False

    except Exception as e:
        logger.error('Error downloading or writing file: {}'.format(e))

    # 4) Sleep five seconds, as a courtesy for using the archive.
    time.sleep(5)

    return success


def download_HRRR(DATE, logger, model='hrrr', field='sfc', hour=range(0, 24),
                  fxx=range(0, 1), OUTDIR='./'):
    """
    Downloads from the University of Utah MesoWest HRRR archive
    Input:
        DATE   - A date object for the model run you are downloading from.
        logger - logger instance
        model  - The model type you want to download. Default is 'hrrr'
                 Model Options are ['hrrr', 'hrrrX','hrrrak']
        field  - Variable fields you wish to download. Default is sfc, surface.
                 Options are fields ['prs', 'sfc','subh', 'nat']
        hour   - Range of model run hours. Default grabs all hours of day.
        fxx    - Range of forecast hours. Default grabs analysis hour (f00).
        OUTDIR - Directory to save the files.

    Outcome:
        Downloads the desired HRRR file and outputs it into the same directory
        structure as NOMADS
    """

    # Loop through each hour and each forecast and download.
    for h in hour:
        for f in fxx:
            # check current time to see if we can run
            # replace utc local with MDT
            # check_before_download(logger)

            fname = "%s.t%02dz.wrf%sf%02d.grib2" % (model, h, field, f)

            success = download_url(fname, OUTDIR, logger,
                                   DATE, model=model, field=field)

            if not success:
                logger.error('Failed to download {} for {}'.format(fname,
                                                                   DATE))


def HRRR_from_UofU(start_date, end_date, save_dir, external_logger=None,
                   forecasts=range(3), model_type='hrrr', var_type='sfc'):
    """
    Download HRRR data from the University of Utah

    Args:
        start_date:         datetime object of start date
        end_date:           datetime object of end date
        save_dir:           base HRRR directory where data will be saved
        external_logger:    logger instance
        forecasts:          forecast hours to get for each hour
        model_type:         model type to download, defaults to hrrr
        var_type:           variable type to download, default to sfc

    Return:

    """

    if external_logger is None:
        logger = utils.create_logger(__name__)

        # fmt = "%(levelname)s: %(msg)s"
        # logger = logging.getLogger(__name__)
        # coloredlogs.install(logger=logger, fmt=fmt)

        msg = "hrrr_archive get data from University of Utah"
        logger.info(msg)
        logger.info("=" * len(msg))

    else:
        logger = external_logger

    # check the start and end date
    if start_date >= end_date:
        logger.error('Start date is before end date')
        raise Exception('Start date is before end date')

    if not isinstance(forecasts, range):
        if not isinstance(forecasts, list):
            logger.error('forecasts must be a list or range')
            raise Exception('forecasts must be a list or range')

    # HRRR data is hourly so create a list of hourly values
    dt_index = pd.date_range(start_date, end_date, freq='H')

    # Make save_dir path if it doesn't exist.
    if not os.path.exists(save_dir):
        raise IOError('save_dir {} does not exist'.format(save_dir))
    logger.info('Writing to {}'.format(save_dir))

    logger.info('Collecting hrrr data for {} through {}'.format(
        start_date, end_date))
    logger.info('Forecast hours: {}'.format(forecasts))
    for dd in dt_index:

        # hour needs to be a list
        hrs = [dd.hour]

        # get the data
        download_HRRR(dd.date(), logger, model=model_type, field=var_type,
                      hour=hrs, fxx=forecasts, OUTDIR=save_dir)

        # pause to be nice
        time.sleep(3)


def cli():
    """
    Command line interface to hrrr_archive
    """

    parser = argparse.ArgumentParser(
        description=("Command line tool for downloading"
                     " HRRR grib files from the University of Utah"))

    parser.add_argument('-s', '--start', dest='start_date',
                        required=True, default=None,
                        help='Datetime to start, ie 2018-07-22 12:00')

    parser.add_argument('-e', '--end', dest='end_date',
                        required=True, default=None,
                        help='Datetime to end, ie 2018-07-22 13:00')

    parser.add_argument('-o', '--output', dest='save_dir',
                        required=True, default=None,
                        help='Path to save the downloaded files to')

    parser.add_argument('-f', '--forecasts', dest='forecasts',
                        required=False, default=1, type=int,
                        help='Number of forecasts to get')

    # start_date, end_date, save_dir, external_logger=None,
    # forecasts=range(3), model_type='hrrr', var_type='sfc'):

    args = parser.parse_args()
    HRRR_from_UofU(
        args.start_date,
        args.end_date,
        args.save_dir,
        forecasts=range(0, args.forecasts)
    )

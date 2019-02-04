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

from datetime import date, datetime, timedelta
import time
import os
import pytz
import requests

# times when downloading should stop as recomended by U of U
tzmdt = pytz.timezone('America/Denver')
no_hours = [0, 3, 6, 9, 12, 15, 18, 21]


def reporthook(a, b, c):
    """
    Report download progress in megabytes
    """
    # ',' at the end of the line is important!i
    print("% 3.1f%% of %.2f MB\r" % (min(100, float(a * b) / c * 100), c/1000000.))


def hrrr_subset(H, half_box=9, lat=40.771, lon=-111.965):
    """
    Cut up the HRRR data based on a center point and the half box surrounding
    the point.
    half_box - number of gridpoints half the size the box surrounding the center point.
    """
    # 1) Compute the abosulte difference between the grid lat/lon and the point
    abslat = np.abs(H['lat']-lat)
    abslon = np.abs(H['lon']-lon)

    # 2) Element-wise maxima. (Plot this with pcolormesh to see what I've done.)
    c = np.maximum(abslon, abslat)

    # 3) The index of the minimum maxima (which is the nearest lat/lon)
    x, y = np.where(c == np.min(c))
    xidx = x[0]
    yidx = y[0]

    print('x:%s, y:%s' % (xidx, yidx))

    subset = {'lat': H['lat'][xidx-half_box:xidx+half_box, yidx-half_box:yidx+half_box],
              'lon': H['lon'][xidx-half_box:xidx+half_box, yidx-half_box:yidx+half_box],
              'value': H['value'][xidx-half_box:xidx+half_box, yidx-half_box:yidx+half_box]}

    return subset


def check_before_download():
    """
    See if it is an okay time to download from U of U based on the times
    that they are pulling data.
    """
    this_hour = nowtime_mdt.time().hour
    this_min = nowtime_mdt.time().minute

    # make sure we are not initiating download at an inconvenient time
    if this_hour in no_hours:
        while this_hour in no_hours and this_min > 30:
            # nowtime = datetime.now()
            nowutc = datetime.utcfromtimestamp(time.time())
            nowtime_mdt = nowutc.replace(tzinfo=pytz.utc).astimezone(tzmdt)
            #print(nowtime_mdt)
            print('Sleeping {}'.format(nowtime_mdt))
            this_hour = nowtime_mdt.time().hour
            this_min = nowtime_mdt.time().minute
            #print(this_hour, this_min)
            time.sleep(100)


def download_url(fname, OUTDIR, logger, file_day, model='hrrr', field='sfc'):
    """
    Construct full URL and download file

    Args:
        fname:      HRRR file name
        OUTDIR:     Location to put HRRR file
        logger:     Logger instance
        file_day:        datetime date correspondig to the hrrr file (i.e hrrr.{date}/hrrr...)

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

    if file_size > 10000:
        print("Downloading:", URL)
        # urllib.urlretrieve(URL, OUTDIR+rename, reporthook)
        new_file = os.path.join(OUTDIR,rename)
        r = requests.get(URL, allow_redirects=True)
        open(new_file, 'wb').write(r.content)
        print("\n")
        success = True
    else:
        # URL returns an "Key does not exist" message
        logger.error("ERROR:", URL, "Does Not Exist")
        success = False

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
        Downloads the desired HRRR file and renames with date info preceeding
        the original file name (i.e. 20170101_hrrr.t00z.wrfsfcf00.grib2)
    """

    # Loop through each hour and each forecast and download.
    for h in hour:
        for f in fxx:
            # check current time to see if we can run
            # replace utc local with MDT
            nowutc = datetime.utcfromtimestamp(time.time())
            tzmdt = pytz.timezone('America/Denver')
            nowtime_mdt = nowutc.replace(tzinfo=pytz.utc).astimezone(tzmdt)

            check_before_download()

            fname = "%s.t%02dz.wrf%sf%02d.grib2" % (model, h, field, f)

            success = download_url(fname, OUTDIR, logger,
                                   DATE, model=model, field=field)

            if not success:
                logger.error('Failed to download {} for {}'.format(fname,
                                                                   DATE))


def HRRR_from_UofU(start_date, end_date, save_dir, logger,
                   hours=range(24), forecasts=range(3)):
    """
    Download HRRR data from the University of Utah

    Args:
        start_date:     datetime object of start date
        end_date:       datetime object of end date
        save_dir:       base HRRR directory where data will be saved
        logger:         logger instance
        hours:          hours that will be downloaded each day
        forecasts:          forecast hours to get for each hour

    Return:

    """

    start_day = start_date.date()
    end_day = start_date.date()

    # SAVEDIR = '/data/snowpack/forecasts/hrrr/'
    SAVEDIR = save_dir

    drange = end_day - start_day
    num_day = drange.days
    # make list of days
    dr = [timedelta(days=d) + start_day for d in range(num_day+1)]
    logger.info('Collecting hrrr data for {} through {}'.format(start_day, end_day))
    logger.info('Writing to {}'.format(SAVEDIR))

    for dd in dr:
        # Start and End Date
        get_this_date = dd
        # Model Type: options include 'hrrr', 'hrrrX', 'hrrrak'
        model_type = 'hrrr'

        # Variable field: options include 'sfc' or 'prs'
        var_type = 'sfc'

        # Make SAVEDIR path if it doesn't exist.
        if not os.path.exists(SAVEDIR):
            raise IOError('SAVEDIR {} does not exist'.format(SAVEDIR))

        # get the data
        download_HRRR(get_this_date, logger, model=model_type, field=var_type,
                      hour=hours, fxx=forecasts, OUTDIR=SAVEDIR)

        logger.info('Wrote files for {} day for {} hours\n'.format(dd, hours))
        # pause to be nice
        time.sleep(3)

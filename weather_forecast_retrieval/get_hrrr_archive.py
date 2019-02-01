"""
Code retrieved from Brian Baylock at University of Utah.
This code allows for downloading of historical HRRR data.

Documentation from the code:
Download archived HRRR files from MesoWest Pando S3 archive system.

Please register before downloading from our HRRR archive:
http://hrrr.chpc.utah.edu/hrrr_download_register.html

For info on the University of Utah HRRR archive and to see what dates are
available, look here:
http://hrrr.chpc.utah.edu/

Contact:
brian.blaylock@utah.edu
"""

# import urllib
# from datetime import date, datetime, timedelta
# from datetime import time as time2
# import time
# import os
# from tzlocal import get_localzone
# import pytz
#
# # times when downloading should stop
# #st_time_interval = timedelta(seconds=1800)
# tzmdt = pytz.timezone('America/Denver')
# no_hours = [0, 3, 6, 9, 12, 15, 18, 21]
# #no_hours = [time2(hour=h) for h in no_hours]
# print(no_hours)
#
# def reporthook(a, b, c):
#     """
#     Report download progress in megabytes
#     """
#     # ',' at the end of the line is important!i
#     print "% 3.1f%% of %.2f MB\r" % (min(100, float(a * b) / c * 100), c/1000000.),
#
#
# def hrrr_subset(H, half_box=9, lat=40.771, lon=-111.965):
#     """
#     Cut up the HRRR data based on a center point and the half box surrounding
#     the point.
#     half_box - number of gridpoints half the size the box surrounding the center point.
#     """
#     # 1) Compute the abosulte difference between the grid lat/lon and the point
#     abslat = np.abs(H['lat']-lat)
#     abslon = np.abs(H['lon']-lon)
#
#     # 2) Element-wise maxima. (Plot this with pcolormesh to see what I've done.)
#     c = np.maximum(abslon, abslat)
#
#     # 3) The index of the minimum maxima (which is the nearest lat/lon)
#     x, y = np.where(c == np.min(c))
#     xidx = x[0]
#     yidx = y[0]
#
#     print 'x:%s, y:%s' % (xidx, yidx)
#
#     subset = {'lat': H['lat'][xidx-half_box:xidx+half_box, yidx-half_box:yidx+half_box],
#               'lon': H['lon'][xidx-half_box:xidx+half_box, yidx-half_box:yidx+half_box],
#               'value': H['value'][xidx-half_box:xidx+half_box, yidx-half_box:yidx+half_box]}
#
#     return subset
#
#
# def download_HRRR(DATE,
#                   model='hrrr',
#                   field='sfc',
#                   hour=range(0, 24),
#                   fxx=range(0, 1),
#                   OUTDIR='./'):
#     """
#     Downloads from the University of Utah MesoWest HRRR archive
#     Input:
#         DATE   - A date object for the model run you are downloading from.
#         model  - The model type you want to download. Default is 'hrrr'
#                  Model Options are ['hrrr', 'hrrrX','hrrrak']
#         field  - Variable fields you wish to download. Default is sfc, surface.
#                  Options are fields ['prs', 'sfc','subh', 'nat']
#         hour   - Range of model run hours. Default grabs all hours of day.
#         fxx    - Range of forecast hours. Default grabs analysis hour (f00).
#         OUTDIR - Directory to save the files.
#
#     Outcome:
#         Downloads the desired HRRR file and renames with date info preceeding
#         the original file name (i.e. 20170101_hrrr.t00z.wrfsfcf00.grib2)
#     """
#
#     # Loop through each hour and each forecast and download.
#     for h in hour:
#         for f in fxx:
#             # check current time to see if we can run
#             # nowtime = datetime.now()
#             # localtz = get_localzone()
#             # replace utc local with MDT
#             nowutc = datetime.utcfromtimestamp(time.time())
#             tzmdt = pytz.timezone('America/Denver')
#             nowtime_mdt = nowutc.replace(tzinfo=pytz.utc).astimezone(tzmdt)
#             #print(nowtime_mdt.time().hour)
#             #print(type(nowtime_mdt.time()))
#             #nowtime_mdt = nowtime.astimezone(pytz.timezone('America/Denver'))
#             this_hour = nowtime_mdt.time().hour
#             this_min = nowtime_mdt.time().minute
#             # wait until not in a bad time
#             #print(this_hour in no_hours)
#             if this_hour in no_hours:
#                 while this_hour in no_hours and this_min > 30:
#                     # nowtime = datetime.now()
#                     nowutc = datetime.utcfromtimestamp(time.time())
#                     nowtime_mdt = nowutc.replace(tzinfo=pytz.utc).astimezone(tzmdt)
#                     #print(nowtime_mdt)
#                     print('Sleeping {}'.format(nowtime_mdt))
#                     this_hour = nowtime_mdt.time().hour
#                     this_min = nowtime_mdt.time().minute
#                     #print(this_hour, this_min)
#                     time.sleep(100)
#
#             # 1) Build the URL string we want to download.
#             #    fname is the file name in the format
#             #    [model].t[hh]z.wrf[field]f[xx].grib2
#             #    i.e. hrrr.t00z.wrfsfcf00.grib2
#             fname = "%s.t%02dz.wrf%sf%02d.grib2" % (model, h, field, f)
#             URL = "https://pando-rgw01.chpc.utah.edu/%s/%s/%s/%s" \
#                    % (model, field, DATE.strftime('%Y%m%d'), fname)
#
#             # 2) Rename file with date preceeding original filename
#             #    i.e. 20170105_hrrr.t00z.wrfsfcf00.grib2
#             rename = "hrrr.%s/%s" \
#                      % (DATE.strftime('%Y%m%d'), fname)
#
#             # create directory if not there
#             redir = os.path.join(OUTDIR, 'hrrr.%s' % (DATE.strftime('%Y%m%d')))
#             if not os.path.exists(redir):
#                 os.makedirs(redir)
#             # 3) Download the file via https
#             # Check the file size, make it's big enough to exist.
#             check_this = urllib.urlopen(URL)
#             file_size = int(check_this.info()['content-length'])
#             if file_size > 10000:
#                 print "Downloading:", URL
#                 urllib.urlretrieve(URL, OUTDIR+rename, reporthook)
#                 print "\n"
#             else:
#                 # URL returns an "Key does not exist" message
#                 print "ERROR:", URL, "Does Not Exist"
#
#             # 4) Sleep five seconds, as a courtesy for using the archive.
#             time.sleep(5)
#
# if __name__ == '__main__':
#
#     # Example downloads all analysis hours for a single day.
#
#     # -------------------------------------------------------------------------
#     # --- Settings: Check online documentation for available dates and hours --
#     # -------------------------------------------------------------------------
#
#     start_day = date(2019, 1, 2)
#     end_day = date(2019, 1, 3)
#
#     SAVEDIR = '/data/snowpack/forecasts/hrrr/'
#     # SAVEDIR = '/mnt/HRRR/'
#     logfile = './hrrr_archive_log.txt'
#     fpl = open(logfile, 'w')
#
#
#     drange = end_day - start_day
#     num_day = drange.days
#     # make list of days
#     dr = [timedelta(days=d) + start_day for d in range(num_day+1)]
#     print('Collecting hrrr data for {} through {}'.format(start_day, end_day))
#     print('Writing to {} and logging to {}'.format(SAVEDIR, logfile))
#     for dd in dr:
#         # Start and End Date
#         # get_this_date = date(2017, 10, 1)
#         get_this_date = dd
#         # Model Type: options include 'hrrr', 'hrrrX', 'hrrrak'
#         model_type = 'hrrr'
#
#         # Variable field: options include 'sfc' or 'prs'
#         # (if you want to initialize WRF with HRRR, you'll need the prs files)
#         var_type = 'sfc'
#
#         # Specify which hours to download
#         # (this example downloads all hours)
#         if model_type == 'hrrrak':
#             # HRRR Alaska run every 3 hours at [0, 3, 6, 9, 12, 15, 18, 21] UTC
#             hours = range(0, 24, 3)
#         else:
#             hours = range(19, 24)
#             #hours = range(15,16)
#             # hours = [2, 3, 23]
#
#         # Specify which forecasts hours to download
#         #forecasts = range(0, 2)
#         forecasts = range(0, 3)
#         #forecasts = [0,1,2]
#         # Specify a Save Directory
#         #SAVEDIR = './HRRR_from_UofU/'
#         # SAVEDIR = os.path.join(SAVEDIR, get_this_date.strftime('%Y%m%d'))
#         # -------------------------------------------------------------------------
#
#         # Make SAVEDIR path if it doesn't exist.
#         if not os.path.exists(SAVEDIR):
#             os.makedirs(SAVEDIR)
#
#         # Call the function to download after checking dates
#         nowdate = datetime.now()
#         print('Downloading {}'.format(dd))
#         download_HRRR(get_this_date, model=model_type, field=var_type,
#                       hour=hours, fxx=forecasts, OUTDIR=SAVEDIR)
#         fpl.write('Wrote files for {} day for {} hours\n'.format(dd, hours))
#
#         time.sleep(3)
#
#     fpl.close()

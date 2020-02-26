"""
Connect to the HRRR site and download the data
"""

import copy
import fnmatch
import logging
import os
import re
from datetime import datetime, time, timedelta
from ftplib import FTP
from logging.handlers import TimedRotatingFileHandler
from multiprocessing.pool import ThreadPool

import numpy as np
import pandas as pd
import requests
import utm
import xarray as xr
from bs4 import BeautifulSoup
from siphon.catalog import TDSCatalog

from weather_forecast_retrieval import utils


class HRRR:
    """
    The High Resolution Rapid Refresh (HRRR) model is a NOAA
    real-time 3 km resolution forecast. The model output is updated
    hourly and assimilates 3km radar. More information can be found
    at https://rapidrefresh.noaa.gov/hrrr/.

    The class will download the 2D surface level products from the ftp
    site. The file format is a grib2 with a model cycle runtime and a
    forecast hour.
    """

    # FTP information
    ftp_url = 'ftp.ncep.noaa.gov'
    ftp_dir = '/pub/data/nccf/com/hrrr/prod'
    date_url = 'hrrr.%Y%m%d'
    file_name = 'hrrr.t*z.wrfsfcf{:02d}.grib2'

    # need to make this hour correct for the forecast
    file_filter = 'hrrr.t*z.wrfsfcf*.grib2'
    regexp = re.compile('hrrr\.t\d\dz\.wrfsfcf\d\d\.grib2')  # noqa

    http_url = 'https://nomads.ncep.noaa.gov/pub/data/nccf/com/hrrr/prod/hrrr.{}/conus/'  # noqa

    # dataset filter by keys arguments
    var_map_grib = {
        'air_temp': {
            'level': 2,
            'typeOfLevel': 'heightAboveGround',
            'cfName': 'air_temperature',
            'cfVarName': 't2m'
        },
        'relative_humidity': {
            'level': 2,
            'typeOfLevel': 'heightAboveGround',
            # 'parameterName': 'Relative humidity',
            'cfVarName': 'r2'
        },
        'wind_u': {
            'level': 10,
            'typeOfLevel': 'heightAboveGround',
            # 'parameterName': 'u-component of wind',
            'cfVarName': 'u10'
        },
        'wind_v': {
            'level': 10,
            'typeOfLevel': 'heightAboveGround',
            # 'parameterName': 'v-component of wind',
            'cfVarName': 'v10'
        },
        'precip_int': {
            'level': 0,
            'typeOfLevel': 'surface',
            'name': 'Total Precipitation',
            'shortName': 'tp'
        },
        'short_wave': {
            'level': 0,
            'typeOfLevel': 'surface',
            'stepType': 'instant',
            'cfVarName': 'dswrf'
        },
        'elevation': {
            'typeOfLevel': 'surface',
            'cfVarName': 'orog'
        }
    }

    # variable map to read the netcdf, the field names are those
    # converted from wgrib2 by default
    var_map_netcdf = {
        'air_temp': 'TMP_2maboveground',
        'dew_point': 'DPT_2maboveground',
        'relative_humidity': 'RH_2maboveground',
        'wind_u': 'UGRD_10maboveground',
        'wind_v': 'VGRD_10maboveground',
        'precip_int': 'APCP_surface',
        'short_wave': 'DSWRF_surface',
        'elevation': 'HGT_surface',
    }

    def __init__(self, configFile=None, external_logger=None):
        """
        Args:
            configFile (str):  path to configuration file.
            external_logger: logger instance if using in part of larger program
        """
        self.num_requests = 2
        self.date_folder = True

        # number of seconds for http request timeout
        self.request_timeout = 600

        # TDS catalog sessions
        self.main_cat = None
        self.day_cat = None

        if configFile is not None:
            self.config = utils.read_config(configFile)

            # parse the rest of the config file
            self.output_dir = self.config['output']['output_dir']

            if 'start_date' in self.config['output'].keys():
                self.start_date = pd.to_datetime(
                    self.config['output']['start_date'])
            if 'end_date' in self.config['output'].keys():
                self.end_date = pd.to_datetime(
                    self.config['output']['end_date'])
            if 'num_requests' in self.config['output'].keys():
                self.num_requests = int(self.config['output']['num_requests'])
            if 'request_timeout' in self.config['output'].keys():
                self.request_timeout = int(
                    self.config['output']['request_timeout'])

        # start logging
        if external_logger is None:

            # setup the logging
            if configFile is not None:
                logfile = None
                if 'log_file' in self.config['logging']:
                    logfile = self.config['logging']['log_file']

                if 'log_level' in self.config['logging']:
                    loglevel = self.config['logging']['log_level'].upper()
                else:
                    loglevel = 'INFO'
            # if no config file use some defaults
            else:
                logfile = None
                loglevel = 'DEBUG'

            numeric_level = getattr(logging, loglevel, None)
            if not isinstance(numeric_level, int):
                raise ValueError('Invalid log level: %s' % loglevel)

            fmt = '%(levelname)s:%(name)s:%(message)s'
            log = logging.getLogger(__name__)
            if logfile is not None:
                # logging.basicConfig(filename=logfile,
                #                     filemode='a',
                #                     level=numeric_level,
                #                     format=fmt)

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

            self._loglevel = numeric_level
            self._logger = log
        else:
            self._logger = external_logger

        # suppress urllib3 connection logging
        logging.getLogger('urllib3').setLevel(logging.WARNING)
        logging.getLogger('cfgrib').setLevel(logging.WARNING)

        msg = '{} -- Initialized HRRR'.format(datetime.now().isoformat())
        self._logger.info("=" * len(msg))
        self._logger.info(msg)

    def __del__(self):
        """
        Clean up the TDS catalog sessions when HRRR is done
        """

        # TDS catalog sessions
        if self.main_cat is not None:
            if hasattr(self.main_cat, 'session'):
                self.main_cat.session.close()
        if self.day_cat is not None:
            if hasattr(self.day_cat, 'session'):
                self.day_cat.session.close()

    def retrieve_ftp(self):
        """
        Retrieve the data from the ftp site. First read the ftp_url and
        determine what dates are available. Then use that to download
        the required data.
        """

        self._logger.info('Retrieving data from the ftp site')
        ftp = FTP(self.ftp_url)

        ftp.connect()
        self._logger.debug('Connected to FTP')

        ftp.login()
        self._logger.debug('Logged into FTP')

        ftp.cwd(self.ftp_dir)
        self._logger.debug('Changed directory to {}'.format(self.ftp_dir))

        # get directory listing on server
        dir_list = ftp.nlst()

        # go through the directory list and see if we need to add
        # any new data files
        for d in dir_list:
            ftp_dir = os.path.join(self.ftp_dir, d, 'conus')
            self._logger.info('Changing directory to {}'.format(ftp_dir))

            # get the files in the new directory
            ftp.cwd(ftp_dir)
            ftp_files = ftp.nlst()

            # check if d exists in output_dir
            out_path = os.path.join(self.output_dir, d)
            if not os.path.isdir(out_path):
                os.mkdir(out_path)
                self._logger.info('mkdir {}'.format(out_path))

            forecast_hours = range(24)
            for fhr in forecast_hours:

                wanted_files = fnmatch.filter(
                    ftp_files, self.file_name.format(fhr))

                self._logger.debug(
                    'Found {} files matching pattern'.format(
                        len(wanted_files)
                    ))

                # go through each file and see if it exists, retrieve if not
                for f in wanted_files:
                    out_file = os.path.join(out_path, f)
                    ftp_file = os.path.join(ftp_dir, f)
                    if not os.path.exists(out_file):
                        self._logger.debug('Retrieving {}'.format(ftp_file))
                        h = open(out_file, 'wb')
                        ftp.retrbinary('RETR {}'.format(ftp_file), h.write)
                        h.close()

        ftp.close()
        self._logger.info(
            '{} -- Done with downloads'.format(datetime.now().isoformat()))

    def retrieve_http_by_date(self, start_date=None, end_date=None):
        """
        Retrieve the data from the ftp site. First read the ftp_url and
        determine what dates are available. Then use that to download
        the required data.

        :params:  start_date - datetime object to override config
                  end_date - datetime object to override config
        """

        self._logger.info('Retrieving data from the http site')

        # could be more robust
        if start_date is not None:
            # if type(start_date) is str:
            start_date = pd.to_datetime(start_date)
            self.start_date = start_date
        if end_date is not None:
            # if type(end_date) is str:
            end_date = pd.to_datetime(end_date)
            self.end_date = end_date

        # check if dates are timezone aware, if not then assume UTC
        if self.start_date.tzinfo is None or \
                self.start_date.tzinfo.utcoffset(self.start_date):
            self.start_date = self.start_date.tz_localize(tz='UTC')
        else:
            self.start_date = self.start_date.tz_convert(tz='UTC')

        if self.end_date.tzinfo is None or \
                self.end_date.tzinfo.utcoffset(self.end_date):
            self.end_date = self.end_date.tz_localize(tz='UTC')
        else:
            self.end_date = self.end_date.tz_convert(tz='UTC')

        diff = pd.Timestamp.utcnow() - self.start_date
        if diff.days > 1:
            # NOAA only keeps the last two days of data
            self._logger.info('Requested start date not within 2 days of now')
            return True

        if self.date_folder:
            d = 'hrrr.{}'.format(self.start_date.strftime('%Y%m%d'))
            # check if d exists in output_dir
            out_path = os.path.join(self.output_dir, d)
            if not os.path.isdir(out_path):
                os.mkdir(out_path)
                self._logger.info('mkdir {}'.format(out_path))
        else:
            out_path = self.output_dir
        self.out_path = out_path

        url_date = self.http_url.format(self.start_date.strftime('%Y%m%d'))

        # get the html text
        self._logger.debug('Requesting html text from {}'.format(url_date))
        page = requests.get(url_date).text

        # convert to BeautifulSoup
        soup = BeautifulSoup(page, 'html.parser')

        # parse
        columns = ['modified', 'name', 'url', 'size']
        df = pd.DataFrame(columns=columns)

        for node in soup.find_all('a'):
            if node.get('href').endswith('grib2'):
                file_name = node.get('href')
                result = self.regexp.match(file_name)

                if result:
                    # matched a file name so get more information about it
                    file_url = url_date + file_name
                    data = node.next_element.next_element.strip()
                    el = data.split(' ')
                    modified = pd.to_datetime(
                        el[0] + ' ' + el[1]).tz_localize(tz='UTC')
                    size = el[3]
                    df = df.append({
                        'modified': modified,
                        'file_name': file_name,
                        'url': file_url,
                        'size': size
                    }, ignore_index=True)

        self._logger.debug('Found {} matching files'.format(len(df)))

        # parse by the date
        idx = (df['modified'] >= self.start_date) & (
            df['modified'] <= self.end_date)
        df = df.loc[idx]
        self._logger.debug(
            'Found {} files between start and end date'.format(len(df)))

        self._logger.debug('Generating requests')
        pool = ThreadPool(processes=self.num_requests)

        self._logger.debug('Sendings {} requests'.format(len(df)))

        # map_async will convert the iterable to a list right away and wait
        # for the requests to finish before continuing
        res = pool.map(self.fetch_url, df.url.to_list())

        self._logger.info(
            '{} -- Done with downloads'.format(datetime.now().isoformat()))
        return res

    def fetch_url(self, uri):
        """
        Fetch the file at the uri and save the file to the out_path

        Args:
            uri: url of the file

        Returns:
            False if failed or path to saved file
        """

        success = False
        try:
            self._logger.debug('Fetching {}'.format(uri))
            r = requests.get(uri, timeout=self.request_timeout)
            if r.status_code == 200:
                f = r.url.split('/')[-1]
                out_file = os.path.join(self.out_path, f)
                with open(out_file, 'wb') as f:
                    f.write(r.content)
                    f.close()
                    self._logger.debug('Saved to {}'.format(out_file))
                    success = out_file

        except Exception as e:
            self._logger.warning('Problem processing response')
            self._logger.warning(e)

        return success

    def get_saved_data(self, start_date, end_date, bbox,
                       file_type='grib2', output_dir=None,
                       var_map=None, forecast=[0], force_zone_number=None,
                       forecast_flag=False, day_hour=0, var_keys=None):
        """
        Get the saved data from above for a particular time and a particular
        bounding box.

        Args:
            start_date:     datetime for the start
            end_date:       datetime for the end
            bbox:           list of  [lonmin,latmin,lonmax,latmax]
            file_type:      'grib' or 'netcdf', determines how to read the file
            var_map:        dictionary to map the desired variables
                            into {new_variable: hrrr_variable}
            forecast:       list of forecast hours to grab
            forecast_flag:  weather or not to get forecast hours
            day_hour:       which hour in the day to grab for forecast scenario
            var_keys:       which keys to grab from smrf variables,
                            default is var_map

        Returns:
            List containing dataframe for the metadata for each node point for
            the desired variables
        """

        if start_date > end_date:
            raise ValueError('start_date before end_date')

        self.start_date = start_date
        self.end_date = end_date
        self.file_type = file_type
        self.bbox = bbox

        if file_type == 'grib2':
            self.get_func = self.get_one_grib
        elif file_type == 'netcdf':
            self.get_func = self.get_one_netcdf
        else:
            raise Exception('Unknown file type argument')

        self._logger.info('getting saved data')
        if var_map is None:
            if file_type == 'grib2':
                var_map = self.var_map_grib
            else:
                var_map = self.var_map_netcdf
            self._logger.warning(
                'var_map not specified, will return default outputs!')

        self.force_zone_number = force_zone_number
        if output_dir is not None:
            self.output_dir = output_dir

        # Don't remember why this was needed, but it does require
        # lots of extra reading
        # self.start_date = self.start_date - timedelta(hours=3)
        # self.end_date = self.end_date + timedelta(hours=3)
        self.delta_hr = timedelta(hours=1)

        # filter to desired keys if specified
        if var_keys is not None:
            new_var_map = {key: var_map[key] for key in var_keys}
        else:
            new_var_map = copy.deepcopy(var_map)
        self.var_map = new_var_map

        # load in the data for the given files and bounding box###

        # get the data for a forecast
        if forecast_flag:
            # df, metadata = self.get_forecast()
            raise NotImplementedError(
                'Getting the forecast is not implemented yet')

        # get the data for a regular run
        else:
            self.get_data()

        # Convert to dataframes and a metadata dataframe
        self.convert_to_dataframes()

        # manipulate data in necessary ways
        for key in self.df.keys():
            self.df[key].sort_index(axis=0, inplace=True)
            if key == 'air_temp':
                self.df['air_temp'] -= 273.15
            if key == 'cloud_factor':
                self.df['cloud_factor'] = 1 - self.df['cloud_factor']/100

        return self.metadata, self.df

    def get_data(self):
        """
        Get the HRRR data

        hours    0    1    2    3    4
                 |----|----|----|----|
        forecast
        start    fx hour
                 |----|----|----|----|
        00       01   02   03   04   05
        01            01   02   03   04
        02                 01   02   03
        03                      01   02

        """
        self.idx = None
        self.metadata = None
        self.df = {}
        self.data = None

        d = self.start_date
        while d <= self.end_date:
            # make sure we get a working file. This allows for 6 tries,
            # accounting for the fact that we start at forecast hour 1
            file_time = d
            for fx_hr in range(1, 8):
                # get the name of the file
                day_folder, file_name = utils.hrrr_file_name_finder(
                    file_time, fx_hr, self.file_type)

                if self.file_type == 'grib2':
                    base_path = os.path.abspath(self.output_dir)
                    fp = os.path.join(base_path, day_folder, file_name)
                elif self.file_type == 'netcdf':
                    fp = [self.output_dir, day_folder, file_name]

                success = self.get_func(fp, self.var_map, file_time)
                # check if we were succesful
                if success:
                    break
                if fx_hr == 6:
                    raise IOError(
                        'Not able to find good grib file for {}'.format(
                            file_time.strftime('%Y-%m-%d %H:%M')))

            d += self.delta_hr

        # return df, metadata

    # def get_forecast(self):
    #     """
    #     Not implemented yet but will get the HRRR forecast
    #     """

    #     # loop through each forecast hour
    #     for f in forecast:
    #         # add forecast hour
    #         file_time = d + pd.to_timedelta(f, 'h')
    #         # make sure we get a working file
    #         for fx_hr in range(7):
    #             fp = utils.hrrr_file_name_finder(self.output_dir,
    #                                              file_time,
    #                                              fx_hr)

    #             success, df, idx, metadata = self.get_one_grib(df, idx,
    #                                                            metadata,
    #                                                            fp,
    #                                                            new_var_map,
    #                                                            file_time,
    #                                                            bbox)
    #             if success:
    #                 break
    #             if fx_hr == 6:
    #                 raise IOError(
    #                     'Not able to find good grib file for {}'.format(
    #                         file_time.strftime('%Y-%m-%d %H:%M')))

    #     return df, metadata

    def convert_to_dataframes(self):
        """
        Convert the xarray's to dataframes to return
        """

        # self.data
        for key, value in self.var_map.items():
            if self.file_type == 'grib2':
                df = self.data[key].to_dataframe()
            else:
                df = self.data[value].to_dataframe()

            # convert from a row multiindex to a column multiindex
            df = df.unstack(level=[1, 2])

            # Get the metadata using the elevation variables
            if key == 'elevation':
                if self.file_type == 'grib2':
                    value = key

                metadata = []
                for mm in ['latitude', 'longitude', value]:
                    dftmp = df[mm].copy()
                    cols = ['grid_{}_{}'.format(x[0], x[1])
                            for x in dftmp.columns.to_flat_index()]
                    dftmp.columns = cols
                    dftmp = dftmp.iloc[0]
                    dftmp.name = mm
                    metadata.append(dftmp)

                self.metadata = pd.concat(metadata, axis=1)
                # it's reporting in degrees from the east
                self.metadata['longitude'] -= 360
                self.metadata = self.metadata.apply(
                    apply_utm, args=(self.force_zone_number,), axis=1)
                self.metadata.rename(columns={value: key}, inplace=True)

            else:
                # else this is just a normal variable
                del df['longitude']
                del df['latitude']

                # make new names for the columns as grid_y_x
                cols = ['grid_{}_{}'.format(x[1], x[2])
                        for x in df.columns.to_flat_index()]
                df.columns = cols
                df.index.rename('date_time', inplace=True)

                # drop any nan values
                df.dropna(axis=1, how='all', inplace=True)
                self.df[key] = df

        # the metadata may have more columns than the dataframes
        c = []
        for key in self.df.keys():
            c.extend(list(self.df[key].columns.values))

        self.metadata = self.metadata[self.metadata.index.isin(list(set(c)))]

    def get_one_netcdf(self, fp, var_map, dt):
        """
        Get valid HRRR data

        Args:
            fp:             Current grib2 file to open
            var_map         Var map of variables to grab
            dt:             datetime represented by the HRRR file

        Returns:
            success:         Boolean representing if the file could be read

        """

        try:
            # instead of opening a session every time, just reuse
            if self.main_cat is None:
                self.main_cat = TDSCatalog(fp[0])

            # have to ensure to change the day catalog if the day changes
            if self.day_cat is None:
                self.day_cat = TDSCatalog(
                    self.main_cat.catalog_refs[fp[1]].href)
            elif self.main_cat.catalog_refs[fp[1]].href != \
                    self.day_cat.catalog_url:
                # close the old session and start a new one
                if hasattr(self.day_cat, 'session'):
                    self.day_cat.session.close()
                self.day_cat = TDSCatalog(
                    self.main_cat.catalog_refs[fp[1]].href)

            if len(self.day_cat.datasets) == 0:
                raise Exception(
                    ("HRRR netcdf THREDDS catalog has"
                     " no datasets for day {}").format(fp[1]))

            # go through and get the file reference
            if fp[2] not in self.day_cat.datasets.keys():
                raise Exception(
                    '{}/{} does not exist on THREDDS server'.format(
                        fp[1], fp[2]))

            d = self.day_cat.datasets[fp[2]]

            self._logger.info('Reading {}'.format(fp[2]))
            data = xr.open_dataset(d.access_urls['OPENDAP'])

            s = data.where((data.latitude >= self.bbox[1]) &
                           (data.latitude <= self.bbox[3]) &
                           (data.longitude >= self.bbox[0]+360) &
                           (data.longitude <= self.bbox[2]+360),
                           drop=True)

            if self.data is None:
                self.data = s
            else:
                self.data = xr.combine_by_coords([self.data, s])

            return True

        except Exception as e:
            self._logger.warning(e)

            return False

    def get_one_grib(self, fp, new_var_map, dt):
        """
        Get valid HRRR data using xarray

        Args:
            fp:             Current grib2 file to open
            new_var_map     Var map of variables to grab
            dt:             datetime represented by the HRRR file

        Returns:
            success:         Boolean representing if the file could be read

        """

        self._logger.debug('Reading {}'.format(fp))

        for key, params in new_var_map.items():

            try:

                # open just one dataset at a time
                data = xr.open_dataset(fp, engine='cfgrib', backend_kwargs={
                                       'filter_by_keys': params})

                if len(data) > 1:
                    raise Exception('More than one grib variable returned')

                # rename the data variable
                if 'cfVarName' in params.keys():
                    data = data.rename({params['cfVarName']: key})
                elif 'shortName' in params.keys():
                    data = data.rename({params['shortName']: key})

                # remove some coordinate so they can all be
                # combined into one dataset
                for v in ['heightAboveGround', 'surface']:
                    if v in data.coords.keys():
                        data = data.drop_vars(v)

                # make the time an index coordinate
                data = data.assign_coords(time=data['valid_time'])
                data = data.expand_dims('time')

                # have to set the x and y coordinates based on
                # the 3000 meter cell size
                data = data.assign_coords(
                    x=np.arange(0, len(data['x'])) * 3000)
                data = data.assign_coords(
                    y=np.arange(0, len(data['y'])) * 3000)

                # delete the step and valid time coordinates
                del data['step']
                del data['valid_time']

                s = data.where((data.latitude >= self.bbox[1]) &
                               (data.latitude <= self.bbox[3]) &
                               (data.longitude >= self.bbox[0]+360) &
                               (data.longitude <= self.bbox[2]+360),
                               drop=True)

                data.close()

                if self.data is None:
                    self.data = s
                else:
                    self.data = xr.combine_by_coords([self.data, s])

                success = True

            except Exception as e:
                self._logger.debug(e)
                self._logger.debug('Moving to next forecast hour')
                success = False

        return success

    # def check_file_health(self, output_dir, start_date, end_date,
    #                       hours=range(23), forecasts=range(18),min_size=100):
    #     """
    #     Check the health of the downloaded hrrr files so that we can download
    #     bad files from U of U archive if something has gone wrong.

    #     Args:
    #         output_dir:     Location of HRRR files
    #         start_date:     date to start checking files
    #         end_date:       date to stop checking files
    #         hours:          hours within the day to check
    #         forecasts:      forecast hours within the day to check

    #     Returns:
    #         files:          list of file names that failed the tests
    #     """
    #     fmt_day = '%Y%m%d'
    #     sd = start_date.date()
    #     ed = end_date.date()
    #     # base pattern template
    #     dir_pattern = os.path.join(output_dir, 'hrrr.{}')
    #     file_pattern_all = 'hrrr.t*z.wrfsfcf*.grib2'
    #     file_pattern = 'hrrr.t{:02d}z.wrfsfcf{:02d}.grib2'
    #     # get a date range
    #     num_days = (ed-sd).days
    #     d_range = [timedelta(days=d) + sd for d in range(num_days)]

    #     # empty list for storing bad files
    #     small_hrrr = []
    #     missing_hrrr = []

    #     for dt in d_range:
    #         # check for files that are too small first
    #         dir_key = dir_pattern.format(dt.strftime(fmt_day))
    #         file_key = file_pattern_all
    #         too_small = health_check.check_min_file_size(dir_key, file_key,
    #                                                      min_size=min_size)
    #         # add bad files to list
    #         small_hrrr += too_small
    #         # check same dirs for missing files
    #         for hr in hours:
    #             for fx in forecasts:
    #                 file_key = file_pattern.format(hr, fx)
    #                 missing = health_check.check_missing_file(
    #                     dir_key, file_key)
    #                 missing_hrrr += missing

    #     # get rid of duplicates
    #     small_hrrr = list(set(small_hrrr))
    #     missing_hrrr = list(set(missing_hrrr))

    #     return small_hrrr, missing_hrrr

    # def fix_bad_files(self, start_date, end_date, out_dir, min_size=100,
    #                   hours=range(23), forecasts=range(18)):
    #     """
    #     Routine for checking the downloaded file health for some files in the
    #     past and attempting to fix the bad file

    #     Args:
    #         start_date:     start date datetime object for checking the files
    #         end_date:       end date datetime object for checking the files
    #         out_dir:        base directory where the HRRR files are stored

    #     """
    #     # get the bad files
    #     small_hrrr, missing_hrrr = self.check_file_health(out_dir,
    #                                                       start_date,
    #                                                       end_date,
    #                                                       min_size=min_size,
    #                                                       hours=hours,
    #                                                       forecasts=forecasts)

    #     if len(missing_hrrr) > 0:
    #         self._logger.info('going to fix missing hrrr')
    #         for fp_mh in missing_hrrr:
    #             self._logger.debug(fp_mh)
    #             print(os.path.basename(fp_mh))
    #             file_day = pd.to_datetime(os.path.dirname(fp_mh)[-8:])
    #             success = hrrr_archive.download_url(os.path.basename(fp_mh),
    #                                                 out_dir,
    #                                                 self._logger,
    #                                                 file_day)

    #         self._logger.info('Finished fixing missing files')

    #     # run through the files and try to fix them
    #     if len(small_hrrr) > 0:
    #         self._logger.info('\n\ngoing to fix small hrrr')
    #         for fp_sh in small_hrrr:
    #             self._logger.info(fp_sh)
    #             file_day = pd.to_datetime(os.path.dirname(fp_sh)[-8:])
    #             # remove and redownload the file
    #             os.remove(fp_sh)
    #             success = hrrr_archive.download_url(os.path.basename(fp_sh),
    #                                                 out_dir,
    #                                                 self._logger,
    #                                                 file_day)

    #             if not success:
    #                 self._logger.warn('Could not download')

    #         self._logger.info('Finished fixing files that were too small')


def apply_utm(s, force_zone_number):
    """
    Calculate the utm from lat/lon for a series
    Args:
        s: pandas series with fields latitude and longitude
        force_zone_number: default None, zone number to force to
    Returns:
        s: pandas series with fields 'X' and 'Y' filled
    """
    p = utm.from_latlon(s.latitude, s.longitude,
                        force_zone_number=force_zone_number)
    s['utm_x'] = p[0]
    s['utm_y'] = p[1]
    return s

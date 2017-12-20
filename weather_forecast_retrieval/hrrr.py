"""
Connect to the HRRR site and download the data
"""

from ftplib import FTP
from urllib.request import urlretrieve
import threading
import os, sys, fnmatch
import logging
import coloredlogs
import pygrib
from datetime import datetime, timedelta
import glob
import pandas as pd
import utm
import numpy as np

from . import utils

PY3 = sys.version_info[0] >= 3
if PY3:  # pragma: no cover
    from configparser import SafeConfigParser
    from urllib.parse import urlencode
else:  # pragma: no cover
    from ConfigParser import SafeConfigParser
    from urllib import urlencode
   

class HRRR():
    """
    The High Resolution Rapid Refresh (HRRR) model is a NOAA
    real-time 3 km resolution forecast. The model output is updated
    hourly and assimilates 3km radar. More information can be found
    at https://rapidrefresh.noaa.gov/hrrr/.
    
    The class will download the 2D surface level products from the ftp
    site. The file format is a grib2 with a model cycle runtime and a
    forecast hour.
    """
    
    ftp_url = 'ftp.ncep.noaa.gov'
    ftp_dir = '/pub/data/nccf/com/hrrr/prod'
    date_url = 'hrrr.%Y%m%d'
    file_name = 'hrrr.t*z.wrfsfcf{:02d}.grib2'
    file_filter = 'hrrr.t*z.wrfsfcf*.grib2'
    output_dir = '/data/snowpack/forecasts/hrrr'
    log_file = os.path.join(output_dir, 'hrrr.log')
    forecast_hours = [0, 1]
    
    # the grib filter page with some of the default parameters
    grib_filter_url = 'http://nomads.ncep.noaa.gov/cgi-bin/filter_hrrr_2d.pl'
    grib_params = {
        'file': 'hrrr.t00z.wrfsfcf00.grib2',
        'lev_10_m_above_ground': 'on',
        'lev_2_m_above_ground': 'on',
        'lev_high_cloud_layer': 'on',
        'lev_low_cloud_layer': 'on',
        'lev_middle_cloud_layer': 'on',
        'lev_surface': 'on',
        'dir': '/hrrr.20171218'
    }
    grib_subregion = {
        'leftlon': -125,
        'rightlon': -103,
        'toplat': 90,
        'bottomlat': 31
        }
    
    num_threads = 20
    
    var_map = {
        'air_temp': {
            'level': 2,
            'parameterName': 'Temperature'
            },
        'relative_humidity': {
            'level': 2,
            'parameterName': 'Relative humidity'
            },
        'wind_u': {
            'level': 10,
            'parameterName': 'u-component of wind'
            },
        'wind_v': {
            'level': 10,
            'parameterName': 'v-component of wind'
            },
        'precip_int': {
            'level': 0,
            'name': 'Total Precipitation'
            },
        'cloud_factor': {
            'parameterName': 'Low cloud cover'
            },
        }
    
    var_elevation = {
        'typeOfLevel': 'surface',
        'parameterName': 'Geopotential height'
        }
    
    def __init__(self, configFile, external_logger=None):
        """
        Args:
            configFile (str):  path to configuration file.
            external_logger: logger instance if using in part of larger program
        """
        
        self.config = utils.read_config(configFile)
        
        
        # start logging
        if external_logger == None:

            if 'log_level' in self.config['logging']:
                loglevel = self.config['logging']['log_level'].upper()
            else:
                loglevel = 'INFO'

            numeric_level = getattr(logging, loglevel, None)
            if not isinstance(numeric_level, int):
                raise ValueError('Invalid log level: %s' % loglevel)

            # setup the logging
            logfile = None
            if 'log_file' in self.config['logging']:
                logfile = self.config['logging']['log_file']

            fmt = '%(levelname)s:%(name)s:%(message)s'
            if logfile is not None:
                logging.basicConfig(filename=logfile,
                                    filemode='w',
                                    level=numeric_level,
                                    format=fmt)
            else:
                logging.basicConfig(level=numeric_level)
                coloredlogs.install(level=numeric_level, fmt=fmt)

            self._loglevel = numeric_level

            self._logger = logging.getLogger(__name__)
        else:
            self._logger = external_logger
        
        self._logger.info('Initialized HRRR')
    
    def retrieve_grib_filter(self):
        """
        Retrieve the data from the http with grib-filter. First read the ftp_url and
        determine what dates are available. Then use that to download
        the required data.
        
        Use the ftp site to get a list of the directories to build the url's
        """
        
        self._logger.info('Retrieving data from the http grib-filter')
        
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
            ftp_dir = os.path.join(self.ftp_dir, d)
            self._logger.info('Changing directory to {}'.format(ftp_dir))
            
            # get the files in the new directory
            ftp.cwd(ftp_dir)
            ftp_files = ftp.nlst()
            
            # check if d exists in output_dir
            out_path = os.path.join(self.output_dir, d)
            if not os.path.isdir(out_path):
                os.mkdir(out_path)
                self._logger.info('mkdir {}'.format(out_path))
            
            # get all the files that match the file filter
            wanted_files = fnmatch.filter(ftp_files, self.file_filter)
            
            # go through each file, see if it exists, and retrieve if not
            threads = []
            for f in wanted_files:
                file_local = os.path.join(out_path, f)
#                 remote_file = os.path.join(ftp_dir, f)
                
                self.grib_params['dir'] = '/{}'.format(d)
                self.grib_params['file'] = f
                p = urlencode(self.grib_params)
                
                remote_url = '{}?{}&subregion=&{}'.format(self.grib_filter_url, p, urlencode(self.grib_subregion))
                
                # get the file if it doesn't exist
                if not os.path.exists(file_local):
                    self._logger.info('Adding {}'.format(f))
                    t = threading.Thread(target=urlretrieve, args=(remote_url, file_local))
                    t.start()
                    threads.append(t)
                    
                if len(threads) == self.num_threads:
                    self._logger.info('Getting {} files'.format(self.num_threads))
                    for t in threads:
                        t.join()
                    threads = []          
            
        ftp.close()
        self._logger.info('Done retrieving files')
        
        
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
            ftp_dir = os.path.join(self.ftp_dir, d)
            self._logger.info('Changing directory to {}'.format(ftp_dir))
            
            # get the files in the new directory
            ftp.cwd(ftp_dir)
            ftp_files = ftp.nlst()
            
            # check if d exists in output_dir
            out_path = os.path.join(self.output_dir, d)
            if not os.path.isdir(out_path):
                os.mkdir(out_path)
                self._logger.info('mkdir {}'.format(out_path))
            
            for fhr in self.forecast_hours:
            
                wanted_files = fnmatch.filter(ftp_files, self.file_name.format(fhr))
                
                self._logger.debug('Found {} files matching pattern'.format(len(wanted_files)))

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
                
              
    def get_saved_data(self, start_date, end_date, bbox, var_map=None, forecast=[0], force_zone_number=None):
        """
        Get the saved data from above for a particular time and a particular
        bounding box.
        
        Args:
            start_date: datetime for the start
            end_date: datetime for the end
            bbox: list of  [lonmin,latmin,lonmax,latmax]
            var_map: dictionary to map the desired variables into {new_variable: hrrr_variable}
            forecast: true or false, whether or not to grab the forecat for the start_date
        
        Returns:
            List containing dataframe for the metadata for each node point for the desired variables
        """
        
        if start_date > end_date:
            raise ValueError('start_date before end_date')
        
        self._logger.info('getting saved data')
        if var_map is None:
            self._logger.warn('var_map not specified, will return all data!')
        
        self.force_zone_number = force_zone_number
        
        # find all the data in between the two dates
        d = start_date.date()
        delta = timedelta(days=1)
        fmatch = []
        while d <= end_date.date():
            # get the path for the upper level directory
            p = d.strftime(self.date_url)
            self._logger.debug('Found directory {}'.format(p))
            
            # find all the files in the directory
            for f in forecast:
                fname = self.file_name.format(f)
                
                pth = os.path.join(self.output_dir, p, fname)
                fmatch += glob.glob(pth)
            
            d += delta
        
        # load in the data for the given files and bounding box
        idx = None
        df = {}
        for f in fmatch:
            self._logger.debug('Reading {}'.format(f))
            gr = pygrib.open(f)
            
            # if this is the first run, then find out a few things about the grid
            if idx is None:
                metadata, idx = self.get_metadata(gr, bbox)
                
                # create all of the dataframes for each mapped variable
                for k in self.var_map.keys():
                    df[k] = pd.DataFrame(columns=metadata.index)
            
            for key,params in self.var_map.items():
                g = gr.select(**params)
                
                if len(g) > 1:
                    raise Exception('variable map returned more than one message for {}'.format(key))
                g = g[0]
             
                # get the data, ensuring that the right location is grabbed
                dt = g.validDate
                if dt >= start_date and dt <= end_date:
                    df[key].loc[dt,:] = g.values[idx]
          
        
        for key in df.keys():
            df[key].sort_index(axis=0, inplace=True)
            if key == 'air_temp':
                df['air_temp'] -= 273.15
                    
        return metadata, df
             
    def get_metadata(self, gr, bbox):
        """
        Generate a metadata dataframe from the elevation data
        
        Args:
            gr: pygrib.open() object
            bbox: list of the bounding box
            
        Returns:
            dataframe for the metadata
        """
        
        elev, lat, lon = gr.select(**self.var_elevation)[0].data()
                
        # subset the data
        idx = (lon >= bbox[0]) & (lon <= bbox[2]) & (lat >= bbox[1]) & (lat <= bbox[3])
        lat = lat[idx]
        lon = lon[idx]
        elev = elev[idx]
        a = np.argwhere(idx)
        primary_id = ['grid_y%i_x%i' % (i[0], i[1]) for i in a]
        self._logger.info('Found {} grid cells within bbox'.format(len(a)))
        
        if len(a) == 0:
            raise ValueError('Did not find any grid cells within bbox')
        
        metadata = pd.DataFrame(index=primary_id,
                                columns=('X', 'Y', 'latitude',
                                         'longitude', 'elevation'))
        metadata['latitude'] = lat.flatten()
        metadata['longitude'] = lon.flatten()
        metadata['elevation'] = elev.flatten()
        metadata = metadata.apply(apply_utm,
                                  args=(self.force_zone_number,),
                                  axis=1)  
        
        return metadata, idx
                
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
    s['X'] = p[0]
    s['Y'] = p[1]
    return s          
        
        
if __name__ == '__main__':
#     HRRR().retrieve_grib_filter()

    start_date = datetime(2017, 12, 1, 10, 0, 0)
    end_date = datetime(2017, 12, 2, 5, 0, 0)
    bbox =  [-120.13, 37.63, -119.06, 38.3]
    HRRR().get_saved_data(start_date, end_date, bbox, force_zone_number=11)
    
    
    
    
    

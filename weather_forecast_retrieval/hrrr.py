"""
Connect to the HRRR ftp site and download the data
"""


from ftplib import FTP
import fnmatch
import os
import logging
import coloredlogs

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
    output_dir = '/data/snowpack/forecasts/hrrr'
    log_file = os.path.join(output_dir, 'hrrr.log')
    forecast_hours = [0, 1]
    
    def __init__(self):
#         # start logging
#         if 'log_level' in self.config['logging']:
#             loglevel = self.config['logging']['log_level'].upper()
#         else:
        loglevel = 'DEBUG'

        numeric_level = getattr(logging, loglevel, None)
        if not isinstance(numeric_level, int):
            raise ValueError('Invalid log level: %s' % loglevel)

        # setup the logging
#         logfile = None
#         if 'log_file' in self.config['logging']:
#        logfile = self.config['logging']['log_file']

        fmt = '%(levelname)s:%(message)s'
#         if logfile is not None:
        logging.basicConfig(filename=self.log_file,
                            filemode='w',
                            level=numeric_level,
                            format=fmt)
#         else:
        logging.basicConfig(level=numeric_level)
#         coloredlogs.install(level=numeric_level, fmt=fmt)

        self._loglevel = numeric_level

        self._logger = logging.getLogger(__name__)
        self._logger.info('Initialized HRRR')
        
        
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
                
        
        
if __name__ == '__main__':
    HRRR().retrieve_ftp()
    
    
    
    
    

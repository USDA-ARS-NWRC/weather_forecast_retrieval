import fnmatch
import os
from datetime import datetime
from ftplib import FTP

from weather_forecast_retrieval import utils


class FtpRetrieval:
    """
    Retrieve the data from the ftp site. First read the ftp_url and
    determine what dates are available. Then use that to download
    the required data.
    """
    URl = 'ftp.ncep.noaa.gov'
    REMOTE_DIR = '/pub/data/nccf/com/hrrr/prod'
    FILE_PATTERN = 'hrrr.t*z.wrfsfcf{:02d}.grib2'

    def __init__(self, output_dir, external_logger=None):
        self.output_dir = output_dir
        self._logger = external_logger or utils.setup_local_logger(__name__)

    def fetch(self):
        self._logger.info('Retrieving data from the ftp site')
        ftp = FTP(FtpRetrieval.URl)

        ftp.connect()
        self._logger.debug('Connected to FTP')

        ftp.login()
        self._logger.debug('Logged into FTP')

        ftp.cwd(FtpRetrieval.REMOTE_DIR)
        self._logger.debug(
            'Changed directory to {}'.format(FtpRetrieval.REMOTE_DIR)
        )

        # get directory listing on server
        dir_list = ftp.nlst()

        # go through the directory list and see if we need to add
        # any new data files
        for d in dir_list:
            ftp_dir = os.path.join(FtpRetrieval.REMOTE_DIR, d, 'conus')
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
                    ftp_files, FtpRetrieval.FILE_PATTERN.format(fhr))

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

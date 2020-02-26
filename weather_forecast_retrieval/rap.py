"""
Connect to the RAP Thredds Data Server site and download the data
"""

import logging
import os
import threading
from urllib.request import urlretrieve

from siphon.catalog import TDSCatalog


class RAP():
    """
    The Rapid Refresh (RAP) model is a NOAA real-time 13km resultion
    reanalysis and forecast product. The mout output is updated
    hourly and creates an 18-hr forecast. This is the larger domain
    for the HRRR model.

    There are two products archives, one for the RAP Analysis and
    one for the RAP forecast. The Analysis has the current hour product
    where the forecast has the 18hrs.

    The class will download the 2D surface level products from the TDS
    site. The file format is a grib2 with a model cycle runtime and a
    forecast hour.
    """

    tds_url = 'https://www.ncei.noaa.gov/thredds/catalog'
    opendap_url = 'https://www.ncei.noaa.gov/thredds/fileServer'
    archive_path = 'rap130anl'
    forecast_path = 'rap130'
#     date_format = '%Y%m%d'

#     file_name = 'hrrr.t*z.wrfsfcf{:02d}.grib2'
    output_dir = '/data/snowpack/forecasts'
    log_file = os.path.join(output_dir, 'rap.log')
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
#         logfile = self.config['logging']['log_file']

        fmt = '%(levelname)s:%(message)s'
#         if logfile is not None:
        logging.basicConfig(filename=self.log_file,
                            filemode='w',
                            level=numeric_level,
                            format=fmt)
#         else:
#         logging.basicConfig(level=numeric_level)
#         coloredlogs.install(level=numeric_level, fmt=fmt)

        self._loglevel = numeric_level

        self._logger = logging.getLogger(__name__)
        self._logger.info('Initialized RAP')

    def retrieve_tds(self):
        """
        Retrieve data from the TDS catalog. There are two levels, one
        for the year month and the other for the day in the month. Follow
        the same format as the TDS and look for the data that is not
        in the local directory.
        """

        self._logger.info('Starting retrieval from RAP arhcive TDS')
        base_url = '{0}/{1}'.format(self.tds_url, self.archive_path)
        opendap_url = '{0}/{1}'.format(self.opendap_url, self.archive_path)
        cat = TDSCatalog('{}/catalog.xml'.format(base_url))

        for lvl in cat.catalog_refs:
            self._logger.debug('Looking in {}'.format(lvl))

            # get the listing
            c1 = TDSCatalog('{}/{}/catalog.xml'.format(base_url, lvl))

            # check if the path exists
            out_path_lvl = os.path.join(
                self.output_dir, self.archive_path, lvl)
            self.check_dir(out_path_lvl)

            for lvl2 in c1.catalog_refs:
                # get the listing
                c2 = TDSCatalog(
                    '{}/{}/{}/catalog.xml'.format(base_url, lvl, lvl2))

                # check if the path exists
                out_path_lvl2 = os.path.join(
                    self.output_dir, self.archive_path, lvl, lvl2)
                self.check_dir(out_path_lvl2)

                # req = []
                threads = []
                for file_name in c2.datasets:
                    # construct the file name locally
                    file_local = os.path.join(out_path_lvl2, file_name)
                    file_remote = '{}/{}/{}/{}'.format(
                        opendap_url, lvl, lvl2, file_name)

                    # get the file if it doesn't exist
                    if not os.path.exists(file_local):
                        self._logger.info('Adding {}'.format(file_name))
                        t = threading.Thread(
                            target=urlretrieve, args=(file_remote, file_local))
                        t.start()
                        threads.append(t)
#                         u = urlretrieve(file_remote, file_local)

                for t in threads:
                    t.join()

    def check_dir(self, p):
        if not os.path.isdir(p):
            os.mkdir(p)
            self._logger.info('mkdir {}'.format(p))


if __name__ == '__main__':
    RAP().retrieve_tds()

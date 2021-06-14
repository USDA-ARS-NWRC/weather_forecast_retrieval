import os
import re
from datetime import datetime
from multiprocessing.pool import ThreadPool

import pandas as pd
import requests
from bs4 import BeautifulSoup

from .config_file import ConfigFile
from .file_handler import FileHandler


class HttpRetrieval(ConfigFile):
    URL = 'https://nomads.ncep.noaa.gov/pub/data/nccf/com/hrrr/prod' \
          '/hrrr.{}/conus/'
    FILE_PATTERN = r'hrrr\.t\d\dz\.wrfsfcf{}.grib2'

    NUMBER_REQUESTS = 2
    REQUEST_TIMEOUT = 600

    def __init__(self, config=None, external_logger=None):
        super().__init__(
            __name__, config=config, external_logger=external_logger
        )

        if self._config is not None and 'output' in self._config:
            if 'num_requests' in self._config['output'].keys():
                self._number_requests = int(
                    self._config['output']['num_requests']
                )
            if 'request_timeout' in self._config['output'].keys():
                self._request_timeout = int(
                    self._config['output']['request_timeout'])

        self.date_folder = True
        self.forecast_hour = None

    @property
    def number_requests(self):
        return getattr(self, '_number_requests', HttpRetrieval.NUMBER_REQUESTS)

    @property
    def request_timeout(self):
        return getattr(self, '_request_timeout', HttpRetrieval.REQUEST_TIMEOUT)

    @property
    def forecast_str(self):
        """Turn a list of forecast hours to strings with 2 digits. For
        example [0, 1] becomes ['00', '01']

        Returns:
            list: None or list of strings
        """
        if self.forecast_hour is None:
            return None
        else:
            return [str(x).zfill(2) for x in self.forecast_hour]

    @property
    def regex_file_name(self):
        """Create the regex string to match the file names

        Returns:
            str: file name pattern to match
        """
        if self.forecast_str is None:
            return self.FILE_PATTERN.format('\\d\\d')
        else:
            return self.FILE_PATTERN.format('({})'.format('|'.join(self.forecast_str)))

    @property
    def url_date(self):
        return HttpRetrieval.URL.format(
            self.start_date.strftime(FileHandler.SINGLE_DAY_FORMAT)
        )

    def fetch_by_date(self, start_date=None, end_date=None, forecast_hour=None):
        """
        :params:  start_date - datetime object to override config
                  end_date - datetime object to override config
        """

        self.log.info('Retrieving data from the http site')

        self.start_date = start_date
        self.end_date = end_date
        self.forecast_hour = forecast_hour

        self.check_dates()

        diff = pd.Timestamp.utcnow() - self.start_date

        if diff.days > 1:
            # NOAA only keeps the last two days of data
            self.log.info('Requested start date not within 2 days of now')
            return True

        if self.date_folder:
            dir = FileHandler.folder_name(self.start_date)
            out_path = os.path.join(self.output_dir, dir)

            if not os.path.isdir(out_path):
                os.mkdir(out_path)
                self.log.info('mkdir {}'.format(out_path))
        else:
            out_path = self.output_dir
        self.out_path = out_path

        df = self.parse_html_for_files()

        # parse by the date
        idx = (df['modified'] >= self.start_date) & \
              (df['modified'] <= self.end_date)
        df = df.loc[idx]
        self.log.debug(
            'Found {} files between start and end date'.format(len(df)))

        self.log.debug('Generating requests')
        pool = ThreadPool(processes=self.number_requests)

        self.log.debug('Sendings {} requests'.format(len(df)))

        # map_async will convert the iterable to a list right away and wait
        # for the requests to finish before continuing
        res = pool.map(self.fetch_from_url, df.url.to_list())

        self.log.info(
            '{} -- Done with downloads'.format(datetime.now().isoformat()))

        return res

    def parse_html_for_files(self):
        """Parse the url from NOMADS with BeautifulSoup and look for matching
        filenames.

        Returns:
            pd.DataFrame: data frame of files that match the pattern
        """

        # get the html text
        self.log.debug('Requesting html text from {}'.format(self.url_date))
        page = requests.get(self.url_date).text

        soup = BeautifulSoup(page, 'html.parser')

        # parse
        columns = ['modified', 'name', 'url', 'size']
        df = pd.DataFrame(columns=columns)

        regex = re.compile(self.regex_file_name)
        for node in soup.find_all('a'):
            if node.get('href').endswith('grib2'):
                file_name = node.get('href')
                result = regex.match(file_name)

                if result:
                    # matched a file name so get more information about it
                    file_url = self.url_date + file_name
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

        self.log.debug('Found {} matching files'.format(len(df)))

        return df

    def fetch_from_url(self, uri):
        """
        Fetch the file at the uri and save the file to the out_path

        Args:
            uri: url of the file

        Returns:
            False if failed or path to saved file
        """

        success = False
        try:
            self.log.debug('Fetching {}'.format(uri))
            r = requests.get(uri, timeout=self.request_timeout)
            if r.status_code == 200:
                f = r.url.split('/')[-1]
                out_file = os.path.join(self.out_path, f)
                with open(out_file, 'wb') as f:
                    f.write(r.content)
                    f.close()
                    self.log.debug('Saved to {}'.format(out_file))
                    success = out_file

        except Exception as e:
            self.log.warning('Problem processing response')
            self.log.warning(e)

        return success

    def check_dates(self):

        # could be more robust
        if self.start_date is not None:
            start_date = pd.to_datetime(self.start_date)
            self.start_date = start_date
        if self.end_date is not None:
            end_date = pd.to_datetime(self.end_date)
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

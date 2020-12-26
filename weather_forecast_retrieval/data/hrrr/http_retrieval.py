import os
import re
from datetime import datetime
from multiprocessing.pool import ThreadPool

import pandas as pd
import requests
from bs4 import BeautifulSoup

from weather_forecast_retrieval import hrrr


class HttpRetrieval(hrrr.HRRR):
    URL = 'https://nomads.ncep.noaa.gov/pub/data/nccf/com/hrrr/prod' \
          '/hrrr.{}/conus/'
    FILE_PATTERN = re.compile(r'hrrr\.t\d\dz\.wrfsfcf\d\d\.grib2')

    NUMBER_REQUESTS = 2
    REQUEST_TIMEOUT = 600

    @property
    def number_requests(self):
        return getattr(self, '_number_requests', HttpRetrieval.NUMBER_REQUESTS)

    @property
    def request_timeout(self):
        return getattr(self, '_request_timeout', HttpRetrieval.REQUEST_TIMEOUT)

    def fetch_by_date(self, start_date=None, end_date=None):
        """
        :params:  start_date - datetime object to override config
                  end_date - datetime object to override config
        """

        self._logger.info('Retrieving data from the http site')

        # could be more robust
        if start_date is not None:
            start_date = pd.to_datetime(start_date)
            self.start_date = start_date
        if end_date is not None:
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
            out_path = os.path.join(self.output_dir, d)

            if not os.path.isdir(out_path):
                os.mkdir(out_path)
                self._logger.info('mkdir {}'.format(out_path))
        else:
            out_path = self.output_dir
        self.out_path = out_path

        url_date = HttpRetrieval.URL.format(self.start_date.strftime('%Y%m%d'))

        # get the html text
        self._logger.debug('Requesting html text from {}'.format(url_date))
        page = requests.get(url_date).text

        soup = BeautifulSoup(page, 'html.parser')

        # parse
        columns = ['modified', 'name', 'url', 'size']
        df = pd.DataFrame(columns=columns)

        for node in soup.find_all('a'):
            if node.get('href').endswith('grib2'):
                file_name = node.get('href')
                result = HttpRetrieval.FILE_PATTERN.match(file_name)

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
        pool = ThreadPool(processes=self.number_requests)

        self._logger.debug('Sendings {} requests'.format(len(df)))

        # map_async will convert the iterable to a list right away and wait
        # for the requests to finish before continuing
        res = pool.map(self.fetch_from_url, df.url.to_list())

        self._logger.info(
            '{} -- Done with downloads'.format(datetime.now().isoformat()))

        return res

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

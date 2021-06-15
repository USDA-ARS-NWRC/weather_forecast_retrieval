import unittest
from datetime import datetime, timedelta
import mock

import pandas as pd

from tests.helpers import skip_external_http_request, mocked_requests_get
from tests.RME import RMETestCase
from weather_forecast_retrieval.data.hrrr import HttpRetrieval


class TestHttpRetrieval(RMETestCase):
    """Test downloading HRRR from NOMADS """

    START_DATE = datetime.utcnow().replace(microsecond=0)
    END_DATE = START_DATE + timedelta(minutes=10)

    def setUp(self):
        super().setUp()
        self.config_file = self.basin_dir.joinpath(
            'hrrr_dates_test.ini'
        ).as_posix()
        self.subject = HttpRetrieval(config=self.config_file)

    def test_start_date(self):
        self.assertEqual(
            pd.to_datetime('2019-07-10 09:00:00'),
            self.subject.start_date,
        )

    def test_end_date(self):
        self.assertEqual(
            pd.to_datetime('2019-07-10 10:00:00'),
            self.subject.end_date,
        )

    def test_num_request(self):
        self.assertEqual(5, self.subject.number_requests)

    def test_forecast_str(self):
        self.subject.forecast_hour = None
        self.assertEqual(self.subject.forecast_str, None)

        self.subject.forecast_hour = [0]
        self.assertEqual(['00'], self.subject.forecast_str)

        self.subject.forecast_hour = [0, 1, 2]
        self.assertEqual(['00', '01', '02'], self.subject.forecast_str)

        self.subject.forecast_hour = [10, 18]
        self.assertEqual(['10', '18'], self.subject.forecast_str)

    def test_compile_file_name(self):

        self.subject.forecast_hour = None
        self.assertEqual(r'hrrr\.t\d\dz\.wrfsfcf\d\d.grib2', self.subject.regex_file_name)

        self.subject.forecast_hour = [0]
        self.assertEqual(r'hrrr\.t\d\dz\.wrfsfcf(00).grib2', self.subject.regex_file_name)

        self.subject.forecast_hour = [0, 1, 2]
        self.assertEqual(r'hrrr\.t\d\dz\.wrfsfcf(00|01|02).grib2',
                         self.subject.regex_file_name)

        self.subject.forecast_hour = [10, 18]
        self.assertEqual(r'hrrr\.t\d\dz\.wrfsfcf(10|18).grib2',
                         self.subject.regex_file_name)

    @mock.patch('requests.get', side_effect=mocked_requests_get)
    def test_parse_html_for_files(self, mock_get):
        df = self.subject.parse_html_for_files()
        self.assertTrue(len(df) == 2)
        self.assertTrue(mock_get.call_count == 1)

        self.subject.forecast_hour = [0]
        df = self.subject.parse_html_for_files()
        self.assertTrue(len(df) == 1)
        self.assertEqual('hrrr.t00z.wrfsfcf00.grib2', df.file_name[0])
        self.assertTrue(mock_get.call_count == 2)

        self.subject.forecast_hour = [0, 1, 2]
        df = self.subject.parse_html_for_files()
        self.assertTrue(len(df) == 2)
        self.assertTrue(mock_get.call_count == 3)

        self.subject.forecast_hour = [10, 18]
        df = self.subject.parse_html_for_files()
        self.assertTrue(len(df) == 0)
        self.assertTrue(mock_get.call_count == 4)

    @unittest.skipIf(
        skip_external_http_request(), 'Skipping HRRR external HTTP requests'
    )
    def test_download_dates(self):
        """ Test loading the data from an OpenDAP THREDDS server """

        try:
            self.subject.fetch_by_date(self.START_DATE, self.END_DATE)
        except Exception as e:
            self.fail('Download failed: {}'.format(e))

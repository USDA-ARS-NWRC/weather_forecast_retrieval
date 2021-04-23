import unittest
from datetime import datetime, timedelta

import pandas as pd

from tests.RME import RMETestCase
from tests.helpers import skip_external_http_request
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
        self.subject = HttpRetrieval(self.config_file)

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

    @unittest.skipIf(
        skip_external_http_request(), 'Skipping HRRR external HTTP requests'
    )
    def test_download_dates(self):
        """ Test loading the data from an OpenDAP THREDDS server """

        try:
            self.subject.fetch_by_date(self.START_DATE, self.END_DATE)
        except Exception as e:
            self.fail('Download failed: {}'.format(e))

import unittest
from datetime import datetime, timedelta

from tests.RME_test_case import RMETestCase
from tests.helpers import skip_external_http_request
from weather_forecast_retrieval.data.hrrr import HttpRetrieval


@unittest.skipIf(
    skip_external_http_request(), 'Skipping HRRR external HTTP requests'
)
class TestHttpRetrieval(RMETestCase):
    """Test downloading HRRR from NOMADS """

    def setUp(self):
        super().setUp()
        self.end_date = datetime.utcnow()
        self.start_date = self.end_date - timedelta(minutes=10)
        self.config_file = self.basin_dir.joinpath(
            'hrrr_dates_test.ini'
        ).as_posix()

    def test_download_dates(self):
        """ Test loading the data from an OpenDAP THREDDS server """

        try:
            HttpRetrieval(self.config_file)\
                .fetch_by_date(self.start_date, self.end_date)
        except Exception as e:
            self.fail('Download failed: {}'.format(e))

from datetime import datetime, timedelta

from tests.RME_test_case import RMETestCase
from weather_forecast_retrieval import hrrr


class TestHRRRRetrieval(RMETestCase):
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
            hrrr.HRRR(self.config_file)\
                .retrieve_http_by_date(self.start_date, self.end_date)
        except Exception as e:
            self.fail('Download failed: {}'.format(e))

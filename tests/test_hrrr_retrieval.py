import os
import shutil
import unittest
from datetime import datetime, timedelta

from weather_forecast_retrieval import hrrr


class TestHRRRRetrevial(unittest.TestCase):
    """Test downloading HRRR from NOMADS """

    def setUp(self):
        self.end_date = datetime.utcnow()
        self.start_date = self.end_date - timedelta(minutes=10)
        self.config_file = 'tests/hrrr_dates_test.ini'

    def tearDown(self):
        """
        Delete the directory created
        """
        out_path = os.path.join(
            'tests/RME/output',
            'hrrr.{}'.format(self.start_date.strftime('%Y%m%d'))
        )
        shutil.rmtree(out_path)

    def test_download_dates(self):
        """ Test loading the data from an OpenDAP THREDDS server """

        try:
            hrrr.HRRR(self.config_file)\
                .retrieve_http_by_date(self.start_date, self.end_date)
        except Exception as e:
            self.fail('Download failed: {}'.format(e))

import mock

from weather_forecast_retrieval.hrrr_nomads import HRRRNOMADS

from tests.RME import RMETestCase
from tests.helpers import mocked_requests_get


class TestHRRRNOMADS(RMETestCase):

    START_DATE = '14-Jun-2021 00:00'
    END_DATE = '14-Jun-2021 02:00'

    def setUp(self):
        super().setUp()
        self.subject = HRRRNOMADS(output_dir=self.output_path)

    def test_init(self):
        self.assertTrue(self.subject.num_requests == 2)
        self.assertFalse(self.subject.verbose)

    @mock.patch('requests.get', side_effect=mocked_requests_get)
    def test_date_range(self, mock_get):
        res = self.subject.date_range(self.START_DATE, self.END_DATE)
        self.assertTrue(len(res) == 2)
        self.assertTrue(mock_get.call_count == 3)

    @mock.patch('requests.get', side_effect=mocked_requests_get)
    def test_date_range_forecast_hr(self, mock_get):
        res = self.subject.date_range(self.START_DATE, self.END_DATE, forecast_hrs=[0, 1])
        self.assertTrue(len(res) == 2)
        self.assertTrue(mock_get.call_count == 3)

    @mock.patch('requests.get', side_effect=mocked_requests_get)
    def test_date_range_forecast_hr_00(self, mock_get):
        res = self.subject.date_range(self.START_DATE, self.END_DATE, forecast_hrs=[0])
        self.assertTrue(len(res) == 1)
        self.assertTrue(mock_get.call_count == 2)

    @mock.patch('requests.get', side_effect=mocked_requests_get)
    def test_latest_no_files(self, mock_get):
        res = self.subject.latest()
        self.assertTrue(len(res) == 0)
        self.assertTrue(mock_get.call_count == 1)

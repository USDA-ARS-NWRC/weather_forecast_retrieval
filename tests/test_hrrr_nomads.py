import mock

from weather_forecast_retrieval.hrrr_nomads import HRRRNOMADS, main, parse_args

from tests.RME import RMETestCase
from tests.helpers import mocked_requests_get

START_DATE = '14-Jun-2021 00:00'
END_DATE = '14-Jun-2021 02:00'
BBOX = [-116.9, 42.9, -116.5, 43.2]


class TestHRRRNOMADS(RMETestCase):

    def setUp(self):
        super().setUp()
        self.subject = HRRRNOMADS(output_dir=self.output_path, verbose=True)

    def test_init(self):
        self.assertTrue(self.subject.num_requests == 2)
        self.assertFalse(self.subject.verbose)

    @mock.patch('requests.get', side_effect=mocked_requests_get)
    def test_date_range(self, mock_get):
        res = self.subject.date_range(START_DATE, END_DATE)
        self.assertTrue(len(res) == 2)
        self.assertTrue(mock_get.call_count == 3)

    @mock.patch('requests.get', side_effect=mocked_requests_get)
    def test_date_range_forecast_hr(self, mock_get):
        res = self.subject.date_range(START_DATE, END_DATE, forecast_hrs=[0, 1])
        self.assertTrue(len(res) == 2)
        self.assertTrue(mock_get.call_count == 3)

    @mock.patch('requests.get', side_effect=mocked_requests_get)
    def test_date_range_forecast_hr_00(self, mock_get):
        res = self.subject.date_range(START_DATE, END_DATE, forecast_hrs=[0])
        self.assertTrue(len(res) == 1)
        self.assertTrue(mock_get.call_count == 2)

    @mock.patch('requests.get', side_effect=mocked_requests_get)
    def test_latest_no_files(self, mock_get):
        res = self.subject.latest()
        self.assertTrue(len(res) == 0)
        self.assertTrue(mock_get.call_count == 1)

    def test_preprocessing(self):

        # override the dates and output dir
        start_date = '2018-07-22 01:00'
        end_date = '2018-07-22 02:00'
        self.subject.output_dir = self.hrrr_dir.as_posix()
        self.subject.set_dates(start_date, end_date, forecast_hrs=[1])

        # The test files don't have TCDC so nothing should be in the folder
        self.subject.preprocessing(BBOX, self.output_path.as_posix())

        output_files = [
            'hrrr.20180722/hrrr.t01z.wrfsfcf01.grib2',
            'hrrr.20180722/hrrr.t02z.wrfsfcf01.grib2',
        ]

        for file in output_files:
            self.assertFalse(
                self.output_path.joinpath(file).exists(),
                'File {} was written although no TCDC variable '
                'in GRIB source file'.format(file)
            )


class TestParseArgs(RMETestCase):

    def test_no_args(self):
        with self.assertRaises(SystemExit) as cm:
            parse_args([])
        self.assertEqual(cm.exception.code, 2)

    def test_output_dir(self):
        args = parse_args([
            '-o',
            self.output_path.as_posix()
        ])
        self.assertEqual(args.latest, 3)
        self.assertEqual(args.num_requests, 2)
        self.assertFalse(args.overwrite)
        self.assertFalse(args.verbose)
        self.assertIsNone(args.forecast_hrs)

    def test_forecast_hrs(self):
        args = parse_args([
            '-o',
            self.output_path.as_posix(),
            '-f',
            '0'
        ])
        self.assertIsInstance(args.forecast_hrs, list)
        self.assertIsInstance(args.forecast_hrs[0], int)

    def test_forecast_hrs_list(self):
        args = parse_args([
            '-o',
            self.output_path.as_posix(),
            '-f',
            '0,1,2'
        ])
        self.assertIsInstance(args.forecast_hrs, list)
        self.assertIsInstance(args.forecast_hrs[0], int)

    def test_overwrite(self):
        args = parse_args([
            '-o',
            self.output_path.as_posix(),
            '--overwrite',
        ])
        self.assertTrue(args.overwrite)

    def test_verbose(self):
        args = parse_args([
            '-o',
            self.output_path.as_posix(),
            '--verbose',
        ])
        self.assertTrue(args.verbose)

    def test_bbox(self):
        args = parse_args([
            '-o',
            self.output_path.as_posix(),
            '--bbox',
            '-116.9, 42.9, -116.5, 43.2'
        ])
        for i, coord in enumerate(args.bbox):
            self.assertEqual(BBOX[i], float(coord))


class TestCli(RMETestCase):

    @mock.patch('requests.get', side_effect=mocked_requests_get)
    def test_latest(self, mock_get):
        args = {
            'output_dir': self.output_path,
            'num_requests': 2,
            'verbose': False,
            'overwrite': False,
            'latest': 3,
            'forecast_hrs': [0, 1],
            'bbox': None,
            'output_path': None
        }
        res = main(**args)

        self.assertTrue(len(res) == 0)
        self.assertTrue(mock_get.call_count == 1)

    @mock.patch('requests.get', side_effect=mocked_requests_get)
    def test_start_end_date(self, mock_get):
        args = {
            'output_dir': self.output_path,
            'num_requests': 2,
            'verbose': False,
            'overwrite': False,
            'start_date': START_DATE,
            'end_date': END_DATE,
            'forecast_hrs': [0, 1],
            'bbox': None,
            'output_path': None
        }
        res = main(**args)

        self.assertTrue(len(res) == 2)
        self.assertTrue(mock_get.call_count == 3)

    @mock.patch('requests.get', side_effect=mocked_requests_get)
    def test_start_end_date_preprocess(self, mock_get):
        args = {
            'output_dir': self.output_path,
            'num_requests': 2,
            'verbose': False,
            'overwrite': False,
            'start_date': START_DATE,
            'end_date': END_DATE,
            'forecast_hrs': [0, 1],
            'bbox': BBOX,
            'output_path': self.output_path.as_posix()
        }
        res = main(**args)

        # Because this is mocked, the grib files don't have anything
        # in them so nothing will really happen, just making sure
        # that the processor is called
        self.assertTrue(len(res) == 2)
        self.assertTrue(mock_get.call_count == 3)

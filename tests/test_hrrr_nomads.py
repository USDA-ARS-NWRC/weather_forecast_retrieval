import mock
import unittest

from tests.helpers import mocked_requests_get, skip_on_github_actions
from tests.RME import RMETestCase
from weather_forecast_retrieval.hrrr_nomads import HRRRNOMADS, main, parse_args

START_DATE = '2019-07-10 09:00:00'
END_DATE = '2019-07-10 10:00:00'
BBOX = [-116.9, 42.9, -116.5, 43.2]


class TestHRRRNOMADS(RMETestCase):

    def setUp(self):
        super().setUp()
        self.subject = HRRRNOMADS(output_dir=self.output_path)

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
        self.assertIsNone(res)
        self.assertTrue(mock_get.call_count == 1)

    @unittest.skipIf(
        skip_on_github_actions(), 'On Github Actions, skipping'
    )
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
    PROCESS_RESULT = mock.Mock('Processor')

    @mock.patch(
        'weather_forecast_retrieval.hrrr_nomads.HRRRNOMADS', autospec=True
    )
    def test_arguments(self, hr_patch):
        args = {
            'output_dir': self.output_path,
            'num_requests': 2,
            'verbose': False,
            'overwrite': False,
            'latest': 3,
            'start_date': None,
            'end_date': None,
            'forecast_hrs': [0, 1],
            'bbox': None,
            'output_path': None
        }

        main(**args)

        hr_patch.assert_called_once_with(
            self.output_path, 2, False, False
        )

    @mock.patch.object(HRRRNOMADS, 'latest', return_value=PROCESS_RESULT)
    def test_latest(self, latest_patch):
        args = {
            'output_dir': self.output_path,
            'num_requests': 2,
            'verbose': False,
            'overwrite': False,
            'latest': 3,
            'start_date': None,
            'end_date': None,
            'forecast_hrs': [0, 1],
            'bbox': None,
            'output_path': None
        }
        result = main(**args)

        self.assertEqual(result, self.PROCESS_RESULT)
        latest_patch.assert_called_once_with(3, [0, 1])

    @mock.patch.object(HRRRNOMADS, 'date_range', return_value=PROCESS_RESULT)
    def test_start_end_date(self, date_range_patch):
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
        result = main(**args)

        self.assertEqual(result, self.PROCESS_RESULT)
        date_range_patch.assert_called_once_with(
            START_DATE, END_DATE, [0, 1]
        )

    @mock.patch.object(HRRRNOMADS, 'preprocessing')
    @mock.patch.object(HRRRNOMADS, 'date_range', return_value=PROCESS_RESULT)
    def test_start_end_date_preprocess(
        self, date_range_patch, preprocessing_patch
    ):
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

        result = main(**args)

        self.assertEqual(result, self.PROCESS_RESULT)
        date_range_patch.assert_called_once_with(
            START_DATE, END_DATE, [0, 1]
        )
        preprocessing_patch.assert_called_once_with(
            BBOX, self.output_path.as_posix()
        )

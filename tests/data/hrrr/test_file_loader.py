import unittest

import mock
import xarray

from tests.RME import RMETestCase
from weather_forecast_retrieval.data.hrrr.file_loader import FileLoader
from weather_forecast_retrieval.data.hrrr.grib_file import GribFile
from weather_forecast_retrieval.data.hrrr.netcdf_file import NetCdfFile


class TestFileLoader(unittest.TestCase):
    def setUp(self):
        self.subject = FileLoader()

    def test_file_loader_property(self):
        self.assertEqual(None, self.subject.file_loader)

        self.subject.file_loader = GribFile

        self.assertEqual(GribFile, self.subject.file_loader)

    def test_file_type(self):
        self.assertEqual(None, self.subject.file_type)

        self.subject.file_loader = GribFile

        self.assertEqual(GribFile.SUFFIX, self.subject.file_type)


def saved_data_return_values():
    metadata = mock.MagicMock()
    metadata.name = 'metadata'
    dataframe = mock.MagicMock(spec={})
    dataframe.name = 'dataframe'
    return metadata, dataframe


@mock.patch.object(FileLoader, 'get_data')
@mock.patch.object(
    FileLoader,
    'convert_to_dataframes',
    return_value=saved_data_return_values()
)
class TestFileLoaderGetSavedData(RMETestCase):
    METHOD_ARGS = [
        RMETestCase.START_DATE, RMETestCase.END_DATE, RMETestCase.BBOX
    ]

    @classmethod
    def setUpClass(cls):
        cls.subject = FileLoader()

    def test_parameters(self, _data_patch, _df_patch):
        self.subject.get_saved_data(*self.METHOD_ARGS, output_dir='path')

        self.assertEqual(self.START_DATE, self.subject.start_date)
        self.assertEqual(self.END_DATE, self.subject.end_date)
        self.assertEqual(self.BBOX, self.subject.file_loader.bbox)
        self.assertEqual('path', self.subject.output_dir)

    def test_defaults_to_grib(self, _data_patch, _df_patch):
        self.subject.get_saved_data(*self.METHOD_ARGS)

        self.assertIsInstance(self.subject.file_loader, GribFile)

    def test_netcdf_parameter(self, _data_patch, _df_patch):
        self.subject.get_saved_data(*self.METHOD_ARGS, file_type='netcdf')

        self.assertIsInstance(self.subject.file_loader, NetCdfFile)

    def test_call_get_data(self, data_patch, _df_patch):
        self.subject.get_saved_data(*self.METHOD_ARGS)

        data_patch.assert_called_once_with(GribFile.VAR_MAP)

    def test_call_get_data_for_specific_keys(self, data_patch, _df_patch):
        var_key = 'air_temp'
        self.subject.get_saved_data(*self.METHOD_ARGS, var_keys=[var_key])

        data_patch.assert_called_once_with(
            {var_key: GribFile.VAR_MAP[var_key]}
        )

    def test_converts_df(self, _data_patch, df_patch):
        self.subject.get_saved_data(*self.METHOD_ARGS)

        df_patch.assert_called_once_with(GribFile.VAR_MAP)

    def test_returns_metadata_and_df(self, _data_patch, _df_patch):
        metadata, dataframe = self.subject.get_saved_data(*self.METHOD_ARGS)

        self.assertEqual('metadata', metadata.name)
        self.assertEqual('dataframe', dataframe.name)


class TestFileLoaderGetData(RMETestCase):
    def setUp(self):
        super().setUp()
        file_loader = mock.MagicMock(spec=GribFile)
        file_loader.name = 'MockLoader'
        file_loader.SUFFIX = GribFile.SUFFIX
        self.file_loader = file_loader

        subject = FileLoader()
        subject.start_date = RMETestCase.START_DATE
        subject.end_date = RMETestCase.END_DATE
        subject.output_dir = RMETestCase.hrrr_dir.as_posix()
        subject.file_loader = file_loader
        self.subject = subject

    def test_call_to_load(self):
        self.subject.get_data({})

        self.assertEqual(
            6,
            self.file_loader.load.call_count,
            msg='More data was loaded than requested forecast hours'
        )
        self.assertRegex(
            self.file_loader.load.call_args.args[0],
            r'.*/hrrr.20180722/hrrr.t05z.wrfsfcf01.grib2',
            msg='Path to file not passed to file loader'
        )
        self.assertEqual(
            {},
            self.file_loader.load.call_args.args[1],
            msg='Var map not passed to file loader'
        )

    def test_tries_six_forecast_hours(self):
        self.subject.file_loader.load.side_effect = Exception('Data error')
        with mock.patch('os.path.exists', return_value=True):
            self.subject.end_date = \
                self.subject.end_date - 5 * FileLoader.NEXT_HOUR

            with self.assertRaisesRegex(IOError, 'Not able to find good file'):
                self.subject.get_data({})

            self.assertEqual(
                6,
                self.file_loader.load.call_count,
                msg='Tried to load than six forecast hours for a '
                    'single time step'
            )

    def test_file_not_found(self):
        self.subject.output_dir = None

        with self.assertRaises(IOError):
            self.subject.get_data({})

        self.assertEqual(
            0,
            self.file_loader.load.call_count,
            msg='Tried to load data from file although not present on disk'
        )

    def test_with_loading_error(self):
        self.subject.file_loader.load.side_effect = Exception('Data error')

        with self.assertRaises(IOError):
            self.subject.get_data({})

        # Can't load the file on disk and the other forecast hours are missing
        self.assertEqual(
            1,
            self.file_loader.load.call_count,
            msg='Tried to find more files than present on disk'
        )

    def test_sets_data_attribute(self):
        self.subject.get_data({})
        self.assertIsInstance(self.subject.data, xarray.Dataset)

    def test_failed_combine_coords(self):
        with mock.patch('xarray.combine_by_coords') as xr_patch:
            xr_patch.side_effect = Exception('Combine failed')
            self.subject.end_date = \
                self.subject.end_date - 5 * FileLoader.NEXT_HOUR

            self.subject.get_data({})

            self.assertEqual(
                None,
                self.subject.data,
                msg='Data set although failed to combine'
            )

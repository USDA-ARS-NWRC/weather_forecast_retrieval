import os
import shutil
import unittest
import xarray as xr
import pandas as pd

from weather_forecast_retrieval.hrrr_preprocessor import HRRRPreprocessor
from weather_forecast_retrieval.hrrr import HRRR


class TestHRRRPreprocessor(unittest.TestCase):
    """Test cropping HRRR files"""

    def setUp(self):
        """
        Test the retrieval of existing data that will be passed to programs
        like SMRF
        """

        self.bbox_crop = [-116.9, 42.9, -116.5, 43.2]
        self.bbox = [-116.85837324, 42.96134124, -116.64913327, 43.16852535]
        self.start_date = '2018-07-22 01:00'
        self.end_date = '2018-07-22 02:00'
        self.force_zone_number = 11
        self.hrrr_directory = 'tests/RME/gridded/hrrr_test/'
        self.output_path = os.path.join('tests', 'RME', 'output')

    def tearDown(self):
        """
        Cleanup the downloaded files
        """

        out_dir = os.path.join(self.output_path, 'hrrr.20180722')
        if os.path.exists(out_dir):
            shutil.rmtree(out_dir)

    def test_pre_process(self):

        HRRRPreprocessor(
            self.hrrr_directory,
            self.start_date,
            self.end_date,
            self.output_path,
            self.bbox_crop,
            1,
            verbose=True
        ).run()

        self.assertTrue(os.path.exists(
            'tests/RME/output/hrrr.20180722/hrrr.t01z.wrfsfcf01.grib2'))
        self.assertTrue(os.path.exists(
            'tests/RME/output/hrrr.20180722/hrrr.t02z.wrfsfcf01.grib2'))

        # ensure that the data has what is needed
        metadata, data = HRRR().get_saved_data(
            pd.to_datetime('2018-07-22 02:00'),
            pd.to_datetime('2018-07-22 03:00'),
            self.bbox,
            file_type='grib2',
            output_dir=self.output_path,
            force_zone_number=self.force_zone_number)

        self.assertListEqual(
            list(data.keys()),
            ['air_temp', 'relative_humidity', 'wind_u', 'wind_v', 'precip_int', 'short_wave']
        )

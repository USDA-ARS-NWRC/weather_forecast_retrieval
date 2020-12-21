import pandas as pd

from tests.RME_test_case import RMETestCase
from weather_forecast_retrieval.hrrr import HRRR
from weather_forecast_retrieval.hrrr_preprocessor import HRRRPreprocessor


class TestHRRRPreprocessor(RMETestCase):
    """Test cropping HRRR files"""

    start_date = '2018-07-22 01:00'
    end_date = '2018-07-22 02:00'

    def setUp(self):
        """
        Test the retrieval of existing data that will be passed to programs
        like SMRF
        """
        super().setUp()
        self.bbox_crop = [-116.9, 42.9, -116.5, 43.2]

    def test_00_pre_process_bad_file(self):

        hp = HRRRPreprocessor(
            self.hrrr_dir.as_posix(),
            self.start_date,
            self.end_date,
            self.output_path.as_posix(),
            self.bbox_crop,
            1,
            verbose=True
        )
        hp.run()

        # neither files have TCDC so they shouldn't write
        self.assertFalse(self.output_path.joinpath(
            'hrrr.20180722/hrrr.t01z.wrfsfcf01.grib2').exists()
        )
        self.assertFalse(self.output_path.joinpath(
            'hrrr.20180722/hrrr.t02z.wrfsfcf01.grib2').exists()
        )

    def test_01_pre_process(self):

        hp = HRRRPreprocessor(
            self.hrrr_dir.as_posix(),
            self.start_date,
            self.end_date,
            self.output_path.as_posix(),
            self.bbox_crop,
            1,
            verbose=True
        )
        # The files in this repo don't have TCDC
        hp.VARIABLES.pop(-1)
        hp.run()

        self.assertTrue(self.output_path.joinpath(
            'hrrr.20180722/hrrr.t01z.wrfsfcf01.grib2').exists()
        )
        self.assertTrue(self.output_path.joinpath(
            'hrrr.20180722/hrrr.t02z.wrfsfcf01.grib2').exists()
        )

        # ensure that the data has what is needed
        metadata, data = HRRR().get_saved_data(
            pd.to_datetime(self.end_date),
            pd.to_datetime('2018-07-22 03:00'),
            self.BBOX,
            file_type='grib2',
            output_dir=self.output_path.as_posix(),
            force_zone_number=self.UTM_ZONE_NUMBER)

        self.assertListEqual(
            list(data.keys()),
            ['air_temp', 'relative_humidity', 'wind_u', 'wind_v', 'precip_int', 'short_wave']
        )

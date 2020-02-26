import unittest

import pandas as pd

from weather_forecast_retrieval import utils


class TestUtils(unittest.TestCase):
    """Test some of the utils"""

    def test_netcdf_filename(self):
        """
        Test that we can find the right file
        """

        file_type = 'netcdf'

        file_time = pd.to_datetime('2018-02-08 05:00')

        fx_hr = 1
        day_folder, file_name = utils.hrrr_file_name_finder(
            file_time, fx_hr, file_type)
        self.assertEqual('hrrr.20180208', day_folder)
        self.assertEqual('hrrr.t04z.wrfsfcf01.nc', file_name)

        fx_hr = 3
        day_folder, file_name = utils.hrrr_file_name_finder(
            file_time, fx_hr, file_type)
        self.assertEqual('hrrr.20180208', day_folder)
        self.assertEqual('hrrr.t02z.wrfsfcf03.nc', file_name)

        # goes back a day
        fx_hr = 8
        day_folder, file_name = utils.hrrr_file_name_finder(
            file_time, fx_hr, file_type)
        self.assertEqual('hrrr.20180207', day_folder)
        self.assertEqual('hrrr.t21z.wrfsfcf08.nc', file_name)

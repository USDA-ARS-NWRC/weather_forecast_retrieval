import unittest
from weather_forecast_retrieval.grib2nc import grib2nc
import os

class TestGrib2nc(unittest.TestCase):
    """Tests for `weather_forecast_retrieval` package."""


    def testGrib2nc(self):
        """
        Convert test data to netcdf
        """

        nc_file = 'tests/RME/output/hrrr.t04z.wrfsfcf01.nc'
        grib_file = 'tests/RME/gridded/hrrr_test/hrrr.20180722/hrrr.t04z.wrfsfcf01.grib2'

        # get the data
        grib2nc(grib_file, nc_file)

        self.assertTrue(os.path.isfile(nc_file))
        os.remove(nc_file)

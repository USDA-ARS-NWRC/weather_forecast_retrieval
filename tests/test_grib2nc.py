import os
import unittest

from weather_forecast_retrieval.grib2nc import grib2nc


class TestGrib2nc(unittest.TestCase):
    """Tests for `weather_forecast_retrieval` package."""

    def testGrib2nc(self):
        """
        Convert test data to netcdf
        """

        nc_file = 'tests/RME/output/hrrr.t04z.wrfsfcf01.nc'
        grib_file = 'tests/RME/gridded/hrrr_test/hrrr.20180722/hrrr.t04z.wrfsfcf01.grib2'  # noqa

        # get the data
        grib2nc(grib_file, nc_file, chunk_x=20, chunk_y=20)

        self.assertTrue(os.path.isfile(nc_file))
        os.remove(nc_file)

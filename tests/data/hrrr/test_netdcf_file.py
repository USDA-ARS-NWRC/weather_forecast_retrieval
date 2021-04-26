import unittest

import tests.helpers
from weather_forecast_retrieval.data.hrrr.netcdf_file import NetCdfFile


class TestNetCdfFile(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.subject = NetCdfFile(
            config=tests.helpers.LOG_ERROR_CONFIG
        )

    def test_log_name(self):
        self.assertRegex(
            self.subject.log.name,
            r'.*netcdf_file$',
        )

    def test_file_suffix(self):
        self.assertEqual('netcdf', NetCdfFile.SUFFIX)

    def test_variable_map(self):
        self.assertEqual(
            NetCdfFile.VAR_MAP,
            self.subject.variable_map
        )

import unittest

import tests.helpers
from weather_forecast_retrieval.data.hrrr.grib_file import GribFile


class TestGribFile(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.subject = GribFile(
            config=tests.helpers.LOG_ERROR_CONFIG
        )

    def test_log_name(self):
        self.assertRegex(
            self.subject.log.name,
            r'.*grib_file$',
        )

    def test_file_suffix(self):
        self.assertEqual('grib2', GribFile.SUFFIX)

    def test_variable_map(self):
        self.assertEqual(
            GribFile.VAR_MAP,
            self.subject.variable_map
        )

    def test_cell_size(self):
        self.assertEqual(3000, GribFile.CELL_SIZE)

    def test_variables(self):
        self.assertEqual(
            GribFile.VAR_MAP.keys(),
            GribFile.VARIABLES
        )

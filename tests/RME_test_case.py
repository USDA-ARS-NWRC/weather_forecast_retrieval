import os
import shutil
import unittest
from pathlib import Path

import pandas as pd


class RMETestCase(unittest.TestCase):
    """
    Base class for all unit tests using the RME data
    """
    test_dir = Path(__file__).parent
    basin_dir = test_dir.joinpath('RME')
    gold_dir = basin_dir.joinpath('gold', 'hrrr')
    hrrr_dir = basin_dir.joinpath('gridded/hrrr_test')
    output_path = basin_dir.joinpath('output')

    START_DATE = pd.to_datetime('2018-07-22 01:00')
    END_DATE = pd.to_datetime('2018-07-22 06:00')
    UTM_ZONE_NUMBER = 11
    BBOX = [-116.85837324, 42.96134124, -116.64913327, 43.16852535]

    def setUp(self):
        """
        Create test directory structure
        Change directory to RME test
        """
        self.output_path.mkdir(exist_ok=True)
        os.chdir(self.basin_dir.as_posix())

    def tearDown(self):
        """
        Cleanup the downloaded files
        Cleanup grib2 index files
        """
        shutil.rmtree(self.output_path)
        for index_file in self.hrrr_dir.rglob('**/*.grib2.*.idx'):
            index_file.unlink()

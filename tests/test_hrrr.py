import os
import unittest

import numpy as np
import pandas as pd

from weather_forecast_retrieval import hrrr


def compare_gold(v_name, gold_dir, test_df):
    """
    Compares two csv files to and determines if they are the same.

    Args:
        v_name: Name with in the file contains
        gold_dir: Directory containing gold standard results
        test_df: Data frame containing test results to be compared
    Returns:
        Boolean: Whether the two images were the same
    """

    # read in the gold standard
    fp1 = os.path.join(gold_dir, v_name+'_data.csv')
    dfgold = pd.read_csv(
        fp1, 'r', delimiter=',', parse_dates=['date_time'], dtype=np.float32
    )
    dfgold.set_index('date_time', inplace=True)

    # see if they are the same
    # result = dfgold.equals(test_df)
    result = np.allclose(test_df.values, dfgold.values, atol=0)

    return result


class TestHRRR(unittest.TestCase):
    """Tests for `weather_forecast_retrieval` package."""

    def setUp(self):
        """
        Test the retrieval of existing data that will be passed to programs
        like SMRF
        """

        # Find the right path to tests
        # check whether or not this is being ran as a single test
        # or part of the suite
        check_file = 'test_hrrr.py'
        if os.path.isfile(check_file):
            self.test_dir = ''
        elif os.path.isfile(os.path.join('tests', check_file)):
            self.test_dir = 'tests'
        else:
            raise Exception('tests directory not found for testing')

        self.test_dir = os.path.abspath(self.test_dir)

        # configurations for testing HRRR.get_saved_data
        self.bbox = [-116.85837324, 42.96134124, -116.64913327, 43.16852535]
        self.start_date = pd.to_datetime('2018-07-22 01:00')
        self.end_date = pd.to_datetime('2018-07-22 06:00')
        self.hrrr_directory = os.path.join(self.test_dir,
                                           'RME/gridded/hrrr_test/')
        self.force_zone_number = 11

        self.output_path = os.path.join(self.test_dir, 'RME', 'output')
        self.gold = os.path.join(self.test_dir, 'RME', 'gold', 'hrrr')

    def testHRRRGribLoad(self):
        """
        Load HRRR data from grib files
        """

        # get the data
        metadata, data = hrrr.HRRR().get_saved_data(
            self.start_date,
            self.end_date,
            self.bbox,
            file_type='grib2',
            output_dir=self.hrrr_directory,
            force_zone_number=self.force_zone_number)

        df = pd.read_csv(os.path.join(self.gold, 'metadata.csv'))
        df.set_index('grid', inplace=True)
        self.assertTrue(
            np.allclose(df.values, metadata[df.columns].values, atol=0)
        )

        # compare with the gold standard
        for k, df in data.items():
            status = compare_gold(k, self.gold, df)
            self.assertTrue(status)

        self.assertTrue(metadata is not None)

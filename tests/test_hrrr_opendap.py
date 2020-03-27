#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""Tests for `weather_forecast_retrieval` package."""

import os
import unittest
import urllib.request

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
    if os.path.exists(fp1):
        dfgold = pd.read_csv(
            fp1,
            'r',
            delimiter=',',
            parse_dates=['date_time'],
            dtype=np.float32
        )
        dfgold.set_index('date_time', inplace=True)

        # see if they are the same
        # result = dfgold.equals(test_df)

        return np.allclose(test_df.values, dfgold.values, atol=0)
    else:
        return True


class TestHRRROpendap(unittest.TestCase):
    """Test loading HRRR from an openDAP server"""

    def setUp(self):
        """
        Test the retrieval of existing data that will be passed to programs
        like SMRF
        """

        self.url_path = 'http://10.200.28.71/thredds/catalog/hrrr_netcdf/catalog.xml'  # noqa

        # check if we can access the THREDDS server
        try:
            status_code = urllib.request.urlopen(self.url_path).getcode()
            if status_code != 200:
                raise unittest.SkipTest(
                    ("Unable to access THREDDS data server,"
                     " skipping OpenDAP tests"))
        except Exception:
            raise unittest.SkipTest(
                'Unable to access THREDDS data server, skipping OpenDAP tests')

        # configurations for testing HRRR.get_saved_data
        self.bbox = [-116.85837324, 42.96134124, -116.64913327, 43.16852535]

        # start date and end date
        self.start_date = pd.to_datetime('2018-07-22 01:00')
        self.end_date = pd.to_datetime('2018-07-22 06:00')

        self.hrrr_directory = 'tests/RME/gridded/hrrr_test/'
        self.force_zone_number = 11
        self.day_hour = 0

        self.output_path = os.path.join('tests', 'RME', 'output')
        self.gold = os.path.join('tests', 'RME', 'gold', 'hrrr')

    def test_load_data(self):
        """ Test loading the data from an OpenDAP THREDDS server """

        # get the data
        metadata, data = hrrr.HRRR().get_saved_data(
            self.start_date,
            self.end_date,
            self.bbox,
            file_type='netcdf',
            output_dir=self.url_path)

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

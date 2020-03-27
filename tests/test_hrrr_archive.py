#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""Tests for `weather_forecast_retrieval` package."""

import os
import shutil
import unittest

import numpy as np
import pandas as pd

from weather_forecast_retrieval import hrrr_archive


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
    fp1 = os.path.join(gold_dir, v_name+'.csv')
    dfgold = pd.read_csv(
        fp1, 'r', delimiter=',', parse_dates=['date_time'], dtype=np.float32
    )
    dfgold.set_index('date_time', inplace=True)

    # see if they are the same
    result = dfgold.equals(test_df)

    return result


class TestHRRRArchive(unittest.TestCase):
    """Test loading HRRR from University of Utah"""

    def setUp(self):
        """
        Test the retrieval of existing data that will be passed to programs
        like SMRF
        """

        # configurations for testing HRRR.get_saved_data
        self.bbox = [-116.85837324, 42.96134124, -116.64913327, 43.16852535]

        # start date and end date
        self.start_date = pd.to_datetime('2018-07-22 12:00')
        self.end_date = pd.to_datetime('2018-07-22 12:10')

        self.hrrr_directory = 'tests/RME/gridded/hrrr_test/'
        self.force_zone_number = 11
        self.day_hour = 0

        self.output_path = os.path.join('tests', 'RME', 'output')
        self.gold = os.path.join('tests', 'RME', 'gold', 'hrrr_opendap')

    def tearDown(self):
        """
        Cleanup the downloaded files
        """

        out_dir = os.path.join(self.output_path, 'hrrr.20180722')
        if os.path.exists(out_dir):
            shutil.rmtree(out_dir)

    def test_archive_errors(self):
        """
        Test some of the simple errors for the archive
        """

        # start end date
        self.assertRaises(
            Exception,
            hrrr_archive.HRRR_from_UofU,
            self.end_date,
            self.start_date,
            self.output_path
        )

        # forecasts
        self.assertRaises(
            Exception,
            hrrr_archive.HRRR_from_UofU,
            self.start_date,
            self.end_date,
            self.output_path,
            forecasts=0
        )

    def test_download_archive(self):
        """
        Test downloading the archive data from UofU.
        Can take around 8 minutes.
        """

        # get the data from the archive
        hrrr_archive.HRRR_from_UofU(
            self.start_date,
            self.end_date,
            self.output_path,
            forecasts=[1]
        )

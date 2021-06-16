#!/usr/bin/env python
# -*- coding: utf-8 -*-

import os

import mock
import numpy as np
import pandas as pd

from tests.helpers import mocked_requests_get
from tests.RME import RMETestCase
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


class TestHRRRArchive(RMETestCase):
    """
    Test loading HRRR from University of Utah

    Test the retrieval of existing data that will be passed to programs
    like SMRF
    """

    start_date = pd.to_datetime('2018-07-22 12:00')
    end_date = pd.to_datetime('2018-07-22 12:10')
    day_hour = 0

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
            self.output_path.as_posix()
        )

        # forecasts
        self.assertRaises(
            Exception,
            hrrr_archive.HRRR_from_UofU,
            self.start_date,
            self.end_date,
            self.output_path.as_posix(),
            forecasts=0
        )

    @mock.patch('requests.get', side_effect=mocked_requests_get)
    def test_download_archive(self, mock_get):

        hrrr_archive.HRRR_from_UofU(
            self.start_date,
            self.end_date,
            self.output_path.as_posix(),
            forecasts=[1]
        )
        self.assertTrue(mock_get.call_count == 1)

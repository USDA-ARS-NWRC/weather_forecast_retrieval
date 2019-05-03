#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""Tests for `weather_forecast_retrieval` package."""

import unittest
from weather_forecast_retrieval import hrrr
import pandas as pd
import os


def compare_csv(v_name,gold_dir,test_dir):
    """
    Compares two csv files to and determines if they are the same.

    Args:
        v_name: Name with in the file contains
        gold_dir: Directory containing gold standard results
        test_dir: Directory containing test results to be compared
    Returns:
        Boolean: Whether the two images were the same
    """
    fp1 = os.path.join(gold_dir,v_name+'.csv')
    fp2 = os.path.join(test_dir,v_name+'.csv')

    dfgold = pd.read_csv(fp1, 'r', delimiter=',', parse_dates=['date_time'])
    dfgold.set_index('date_time', inplace=True)

    dfnew = pd.read_csv(fp2, 'r', delimiter=',', parse_dates=['date_time'])
    dfnew.set_index('date_time', inplace=True)

    # see if they are the same
    result = dfgold.equals(dfnew)

    return  result


class TestHRRR(unittest.TestCase):
    """Tests for `weather_forecast_retrieval` package."""

    def setUp(self):
        """
        Test the retrieval of existing data that will be passed to programs
        like SMRF
        """

        # Find the right path to tests
        # check whether or not this is being ran as a single test or part of the suite
        check_file = 'test_hrrr.py'
        if os.path.isfile(check_file):
            self.test_dir = ''
        elif os.path.isfile(os.path.join('tests', check_file)):
            self.test_dir = 'tests'
        else:
            raise Exception('tests directory not found for testing')

        self.test_dir = os.path.abspath(self.test_dir)
        ### configurations for testing HRRR.get_saved_data
        self.bbox = [-116.85837324, 42.96134124, -116.64913327, 43.16852535]
        self.start_date = pd.to_datetime('2018-07-22 12:00')
        self.end_date = pd.to_datetime('2018-07-22 17:00')
        self.hrrr_directory = os.path.join(self.test_dir,
                                           'RME/gridded/hrrr_test/')
        self.force_zone_number = 11
        self.day_hour = 0

        self.output_path = os.path.join(self.test_dir,'RME','output')
        self.gold = os.path.join(self.test_dir,'RME','gold','hrrr')

        # read and write the hrrr data
        # self.readNormalHRRR()

        fcast = [0]
        self.forecast_flag = False

        self.hrrr = hrrr.HRRR()
        # get the data
        metadata, data = self.hrrr.get_saved_data(
                                        self.start_date,
                                        self.end_date,
                                        self.bbox,
                                        output_dir=self.hrrr_directory,
                                        force_zone_number=self.force_zone_number,
                                        forecast=fcast,
                                        forecast_flag=self.forecast_flag,
                                        day_hour=self.day_hour)

        # write the data to csv
        for k, v in data.items():
            data_path = os.path.join(self.output_path, '{}_data.csv'.format(k))
            v.to_csv(data_path, index_label='date_time')

    def testAirTemp(self):
        """
        Compare the air temp DataFrame
        """
        a = compare_csv('air_temp_data', self.gold, self.output_path)
        assert(a)

    def testPrecip(self):
        """
        Compare the precip DataFrame
        """
        a = compare_csv('precip_int_data', self.gold, self.output_path)
        assert(a)

    def testRH(self):
        """
        Compare the Relative Humidity DataFrame
        """
        a = compare_csv('relative_humidity_data', self.gold, self.output_path)
        assert(a)

    def testWindU(self):
        """
        Compare the U Wind Speed DataFrame
        """
        a = compare_csv('wind_u_data', self.gold, self.output_path)
        assert(a)

    def testWindV(self):
        """
        Compare the V Wind Speed DataFrame
        """
        a = compare_csv('wind_v_data', self.gold, self.output_path)
        assert(a)

    def testShortWave(self):
        """
        Compare the Short Wave DataFrame
        """
        a = compare_csv('short_wave_data', self.gold, self.output_path)
        assert(a)

    # def test_fcast_hrrr(self):
    #     """Test something."""
    #     self.fcast = range(1,5)
    #     self.forecast_flag = True
    #
    #     assert result

if __name__ == '__main__':
    unittest.main()

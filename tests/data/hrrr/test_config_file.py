import logging
import unittest

import mock
import pandas as pd

from weather_forecast_retrieval.data.hrrr.config_file import ConfigFile


class TestConfigFile(unittest.TestCase):
    LOGGER_NAME = 'ConfigFileTest'
    CONFIG = {
        'output': {
            'output_dir': 'output_location',
            'start_date': '2020-12-31 00:00',
            'end_date': '2020-12-31 23:00',
        }
    }

    @classmethod
    def setUpClass(cls):
        with mock.patch(
            'weather_forecast_retrieval.utils.read_config',
            return_value=cls.CONFIG
        ):
            cls.subject = ConfigFile(cls.LOGGER_NAME, cls.CONFIG)

    def test_output_dir(self):
        self.assertEqual(
            self.CONFIG['output']['output_dir'],
            self.subject.output_dir
        )

    def test_start_date(self):
        self.assertEqual(
            pd.to_datetime(self.CONFIG['output']['start_date']),
            self.subject.start_date,
        )

    def test_end_date(self):
        self.assertEqual(
            pd.to_datetime(self.CONFIG['output']['end_date']),
            self.subject.end_date
        )

    def test_no_config(self):
        subject = ConfigFile(self.LOGGER_NAME)
        self.assertIsNone(subject.config)

    def test_default_properties(self):
        subject = ConfigFile(self.LOGGER_NAME)
        self.assertIsNone(subject.start_date)
        self.assertIsNone(subject.end_date)
        self.assertIsNone(subject.output_dir)

    def test_logger_name(self):
        self.assertEqual(self.LOGGER_NAME, self.subject.log.name)

    def test_external_logger(self):
        external_logger = logging.Logger('External')
        subject = ConfigFile(
            self.LOGGER_NAME, external_logger=external_logger
        )
        self.assertEqual(external_logger.name, subject.log.name)

    def test_log_property(self):
        self.assertIsInstance(self.subject.log, logging.Logger)

import unittest

from weather_forecast_retrieval.data.hrrr.base_file import BaseFile


class TestBaseFile(unittest.TestCase):
    LOGGER_NAME = 'BaseFileTest'

    @classmethod
    def setUpClass(cls):
        cls.subject = BaseFile(cls.LOGGER_NAME)

    def test_bbox_property(self):
        self.assertIsNone(self.subject.bbox)

        self.subject.bbox = []

        self.assertEqual([], self.subject.bbox)

    def test_has_log_property(self):
        self.assertEqual(self.LOGGER_NAME, self.subject.log.name)

    def test_variable_map_property(self):
        self.assertEqual(
            BaseFile.VAR_MAP,
            self.subject.variable_map
        )

    def test_load_method(self):
        with self.assertRaises(NotImplementedError):
            self.subject.load(None)

import copy

from weather_forecast_retrieval.data.hrrr.config_file import ConfigFile


class BaseFile(ConfigFile):
    """
    Base Class for local HRRR file loaders to inherit from.
    Ensures required properties and methods.
    """
    VAR_MAP = None

    def __init__(self, logger_name, config=None, external_logger=None):
        super().__init__(
            logger_name, config=config, external_logger=external_logger
        )

        self._bbox = None

    @property
    def bbox(self):
        return self._bbox

    @bbox.setter
    def bbox(self, value):
        self._bbox = value

    @property
    def variable_map(self):
        return copy.deepcopy(self.VAR_MAP)

    def load(self, *args):
        raise NotImplementedError(
            'Method {} needs to be implemented in sub-class'.format(__name__)
        )

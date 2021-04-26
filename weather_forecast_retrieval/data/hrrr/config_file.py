import logging
from datetime import datetime

import pandas as pd

from weather_forecast_retrieval import utils


class ConfigFile:
    """
    Class containing logic to parse a config with the basic inputs of:
      * output_dir
      * start_date
      * end_date
    These are also set as attributes on the class. File argument can be a file
    on disk or a dictionary.

    Also sets up a logger and has property to use for logging messages to that.
    Logging specifications can be part of the configuration as well.
    """
    LOG_INIT_MESSAGE = ' -- Initialized at: {} --'

    def __init__(self, logger_name, config=None, external_logger=None):
        """
        Args:
            logger_name:     (Required) Name to use in log file
            config:          Path to configuration file or a dictionary
            external_logger: Logger instance to use instead of a new Logger
        """
        self.start_date = None
        self.end_date = None
        self.output_dir = None
        self.__parse_config(config)

        self._logger = external_logger or \
            self.__log_initialization(logger_name)

        # suppress urllib3 connection logging
        logging.getLogger('urllib3').setLevel(logging.WARNING)
        logging.getLogger('cfgrib').setLevel(logging.WARNING)

    @property
    def log(self):
        return self._logger

    def __parse_config(self, config):
        self._config = config

        if config is None:
            return
        elif isinstance(config, dict):
            self._config = config
        else:
            self._config = utils.read_config(config)

        if 'output' in self._config:
            keys = self._config['output'].keys()

            if 'output_dir' in keys:
                self.output_dir = self._config['output']['output_dir']

            if 'start_date' in keys:
                self.start_date = pd.to_datetime(
                    self._config['output']['start_date'])

            if 'end_date' in keys:
                self.end_date = pd.to_datetime(
                    self._config['output']['end_date'])

    def __log_initialization(self, logger_name):
        log = utils.setup_local_logger(logger_name, self._config)

        msg = self.LOG_INIT_MESSAGE.format(
            datetime.now().replace(microsecond=0).isoformat()
        )
        log.info("=" * len(msg))
        log.info(msg)

        return log

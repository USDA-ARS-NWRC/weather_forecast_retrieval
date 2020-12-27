import logging
from datetime import datetime

import pandas as pd

from weather_forecast_retrieval import utils


class ConfigFile:
    """
    Class containing logic to parse a config file with the basic inputs of:
      * output_dir
      * start_date
      * end_date
    These are also set as attributes on the class.

    Also sets up a logger and has property to use for logging messages to that.
    """
    LOG_INIT_MESSAGE = ' -- Initialized at: {} --'

    def __init__(self, logger_name, config_file=None, external_logger=None):
        """
        Args:
            logger_name:     (Required) Name to use in log file
            config_file:     Path to configuration file.
            external_logger: Logger instance if using in part of larger
                             program
        """
        self.config = None
        if config_file is not None:
            self.__parse_config(config_file)

        self._logger = external_logger or \
            utils.setup_local_logger(logger_name, self.config)

        # suppress urllib3 connection logging
        logging.getLogger('urllib3').setLevel(logging.WARNING)
        logging.getLogger('cfgrib').setLevel(logging.WARNING)

        self.__log_initialization()

    @property
    def log(self):
        return self._logger

    def __parse_config(self, file):
        self.config = utils.read_config(file)

        # parse the rest of the config file
        self.output_dir = self.config['output']['output_dir']

        if 'start_date' in self.config['output'].keys():
            self.start_date = pd.to_datetime(
                self.config['output']['start_date'])
        if 'end_date' in self.config['output'].keys():
            self.end_date = pd.to_datetime(
                self.config['output']['end_date'])

    def __log_initialization(self):
        msg = self.LOG_INIT_MESSAGE.format(
            datetime.now().replace(microsecond=0).isoformat()
        )
        self.log.info("=" * len(msg))
        self.log.info(msg)

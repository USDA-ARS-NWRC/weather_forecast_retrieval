import logging
import os
import sys
from collections import Sequence
from configparser import ConfigParser
from datetime import time
from logging.handlers import TimedRotatingFileHandler

import coloredlogs

basestring = str
unicode_type = str

try:
    from cyordereddict import OrderedDict
except ImportError:  # pragma: no cover
    try:
        from collections import OrderedDict
    except ImportError:
        from ordereddict import OrderedDict


def setup_local_logger(name, config=None):
    # Defaults
    logfile = None
    loglevel = 'DEBUG'
    message_format = '%(levelname)s:%(name)s: %(message)s'
    log = logging.getLogger(name)

    if config is not None and 'logging' in config:
        if 'log_file' in config['logging']:
            logfile = config['logging']['log_file']

        if 'log_level' in config['logging']:
            loglevel = config['logging']['log_level'].upper()
        else:
            loglevel = 'INFO'

    numeric_level = getattr(logging, loglevel, None)
    if not isinstance(numeric_level, int):
        raise ValueError('Invalid log level: %s' % loglevel)

    if logfile is not None:
        handler = TimedRotatingFileHandler(logfile,
                                           when='D',
                                           interval=1,
                                           utc=True,
                                           atTime=time(),
                                           backupCount=30)
        log.setLevel(numeric_level)
        formatter = logging.Formatter(message_format)
        handler.setFormatter(formatter)
        log.addHandler(handler)
    else:
        coloredlogs.install(
            logger=log, level=numeric_level, fmt=message_format
        )

    return log


def read_config(config_file, encoding='utf-8'):
    """
    Returns a dictionary with subdictionaries of all configFile options/values

    Args:
        config_file - String path to the config file to be opened.

    Returns:
        dict1: A dictionary of dictionaires representing the config file.
    """

    config = ConfigParser()
    config.optionxform = str

    PY3 = sys.version_info[0] >= 3

    if PY3:
        config.read(config_file, encoding=encoding)
    else:
        config.read(config_file)

    sections = config.sections()
    dict1 = OrderedDict()
    for section in sections:
        options = config.options(section)
        dict2 = OrderedDict()
        for option in options:
            dict2[option.lower()] = config_type(config.get(section, option))
        dict1[section.lower()] = dict2

    return dict1


def config_type(value):
    """
    Parse the type of the configuration file option.
    First see the value is a bool, then try float, finally return a string.
    """
    if not isinstance(value, list):
        val_list = [x.strip() for x in value.split(',')]
    else:
        val_list = value
    ret_list = []

    for value in val_list:
        if value.lower() in ['true', 't']:  # True
            ret_list.append(True)
        elif value.lower() in ['false', 'f']:  # False
            ret_list.append(False)
        elif value.lower() in ['none', '']:  # None
            ret_list.append(None)
        elif isint(value):  # int
            ret_list.append(int(value))
        elif isfloat(value):  # float
            ret_list.append(float(value))
        else:  # string or similar
            ret_list.append(os.path.expandvars(value))

    if len(ret_list) > 1:
        return ret_list
    else:
        return ret_list[0]


def isbool(x):
    '''Test if str is an bolean'''
    if isinstance(x, float) or isinstance(x, basestring) and '.' in x:
        return False
    try:
        a = float(x)
        b = int(a)
    except ValueError:
        return False
    else:
        return a == b


def isfloat(x):
    '''Test if value is a float'''
    try:
        float(x)
    except ValueError:
        return False
    else:
        return True


def isint(x):
    '''Test if value is an integer'''
    if isinstance(x, float) or isinstance(x, basestring) and '.' in x:
        return False
    try:
        a = float(x)
        b = int(a)
    except ValueError:
        return False
    else:
        return a == b


def isscalar(x):
    '''Test if a value is a scalar'''
    if isinstance(x, (Sequence, basestring, unicode_type)):
        return False
    else:
        return True

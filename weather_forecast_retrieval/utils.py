
import os, sys
from collections import Sequence
import datetime
import pandas as pd

PY3 = sys.version_info[0] >= 3

if PY3:  # pragma: no cover
    from configparser import SafeConfigParser
    basestring = str
    unicode_type = str
else:  # pragma: no cover
    from ConfigParser import SafeConfigParser
    basestring = basestring
    unicode_type = unicode

try:
    from cyordereddict import OrderedDict
except ImportError:  # pragma: no cover
    try:
        from collections import OrderedDict
    except ImportError:
        from ordereddict import OrderedDict

def read_config(config_file, encoding='utf-8'):
    """
    Returns a dictionary with subdictionaries of all configFile options/values

    Args:
        config_file - String path to the config file to be opened.

    Returns:
        dict1: A dictionary of dictionaires representing the config file.
    """

    config = SafeConfigParser()
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


def get_hrrr_file_date(fp, fx=False):
    '''
    Get the date from a hrrr file name. Assuming the directory structure
    used in the rest of this code.
    
    Args:
        fp: file path to hrrr grib2 file within normal hrrr structure
        fx: include the forecast hour or not
    Returns:
        file_time: datetime object for that specific file

    '''
    # go off the base and fx hour or just the base hour
    fn = os.path.basename(fp)
    if fx:
        add_hrs = int(fn[6:8]) + int(fn[17:19])
    else:
        add_hrs = int(fn[6:8])

    # find the day from the hrrr.<day> folder
    date_day = pd.to_datetime(os.path.dirname(fp).split('hrrr.')[1])
    # find the actual datetime
    file_time = pd.to_datetime(date_day + datetime.timedelta(hours=add_hrs))

    return file_time

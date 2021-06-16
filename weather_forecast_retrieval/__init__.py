from pkg_resources import DistributionNotFound, get_distribution

import weather_forecast_retrieval.data

try:
    __version__ = get_distribution(__name__).version
except DistributionNotFound:
    __version__ = 'unknown'

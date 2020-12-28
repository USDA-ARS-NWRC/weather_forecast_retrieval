import numpy as np
import xarray as xr

from weather_forecast_retrieval.data.hrrr.base_file import BaseFile


class GribFile(BaseFile):
    SUFFIX = 'grib2'

    CELL_SIZE = 3000  # in meters

    # dataset filter by keys arguments
    VAR_MAP = {
        'air_temp': {
            'level': 2,
            'typeOfLevel': 'heightAboveGround',
            'cfName': 'air_temperature',
            'cfVarName': 't2m'
        },
        'relative_humidity': {
            'level': 2,
            'typeOfLevel': 'heightAboveGround',
            # 'parameterName': 'Relative humidity',
            'cfVarName': 'r2'
        },
        'wind_u': {
            'level': 10,
            'typeOfLevel': 'heightAboveGround',
            # 'parameterName': 'u-component of wind',
            'cfVarName': 'u10'
        },
        'wind_v': {
            'level': 10,
            'typeOfLevel': 'heightAboveGround',
            # 'parameterName': 'v-component of wind',
            'cfVarName': 'v10'
        },
        'precip_int': {
            'level': 0,
            'typeOfLevel': 'surface',
            'name': 'Total Precipitation',
            'shortName': 'tp'
        },
        'short_wave': {
            'level': 0,
            'typeOfLevel': 'surface',
            'stepType': 'instant',
            'cfVarName': 'dswrf'
        },
        'elevation': {
            'typeOfLevel': 'surface',
            'cfVarName': 'orog'
        }
    }

    def __init__(self, config_file=None, external_logger=None):
        super().__init__(
            __name__, config_file=config_file, external_logger=external_logger
        )

    def load(self, file, var_map):
        """
        Get valid HRRR data using Xarray

        Args:
            file:    Path to grib2 file to open
            var_map: Var map of variables to grab

        Returns:
            Array with Xarray Datasets for each variable and
            cropped to bounding box
        """

        variable_data = []

        self.log.debug('Reading {}'.format(file))

        # open just one dataset at a time
        for key, params in var_map.items():
            data = xr.open_dataset(
                file,
                engine='cfgrib',
                backend_kwargs={'filter_by_keys': params}
            )

            if len(data) > 1:
                raise Exception('More than one grib variable returned')

            # rename the data variable
            if 'cfVarName' in params.keys():
                data = data.rename({params['cfVarName']: key})
            elif 'shortName' in params.keys():
                data = data.rename({params['shortName']: key})

            # remove some coordinate so they can all be
            # combined into one dataset
            for v in ['heightAboveGround', 'surface']:
                if v in data.coords.keys():
                    data = data.drop_vars(v)

            # make the time an index coordinate
            data = data.assign_coords(time=data['valid_time'])
            data = data.expand_dims('time')

            # have to set the x and y coordinates based on the cell size
            data = data.assign_coords(
                x=np.arange(0, len(data['x'])) * self.CELL_SIZE)
            data = data.assign_coords(
                y=np.arange(0, len(data['y'])) * self.CELL_SIZE)

            # delete the step and valid time coordinates
            del data['step']
            del data['valid_time']

            variable_data.append(
                data.where(
                    (data.latitude >= self.bbox[1]) &
                    (data.latitude <= self.bbox[3]) &
                    (data.longitude >= self.bbox[0] + 360) &
                    (data.longitude <= self.bbox[2] + 360),
                    drop=True
                )
            )

            data.close()

        return variable_data

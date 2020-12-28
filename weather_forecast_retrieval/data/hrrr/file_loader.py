import os
from datetime import timedelta

import pandas as pd
import utm
import xarray as xr

from .config_file import ConfigFile
from .file_handler import FileHandler
from .grib_file import GribFile
from .netcdf_file import NetCdfFile


class FileLoader(ConfigFile):
    """
    Load data from local HRRR files.
    Currently supports loading from Grib or NetCDF format.
    """
    # Maximum hour that local files will be attempted to be read if a previous
    # hour could not be found or successfully loaded.
    MAX_FORECAST_HOUR = 6
    NEXT_HOUR = timedelta(hours=1)

    def __init__(self, config_file=None, external_logger=None):
        super().__init__(
            __name__, config_file=config_file, external_logger=external_logger
        )

        self.start_date = None
        self.end_date = None

        self._file_loader = None
        self.data = None
        self.force_zone_number = None

    @property
    def file_loader(self):
        return self._file_loader

    @file_loader.setter
    def file_loader(self, value):
        self._file_loader = value

    @property
    def file_type(self):
        if self._file_loader is not None:
            return self._file_loader.SUFFIX
        else:
            return None

    def get_saved_data(self,
                       start_date, end_date, bbox,
                       output_dir=None, file_type='grib2',
                       force_zone_number=None,
                       forecast_flag=False,
                       var_keys=None):
        """
        Get the saved data from above for a particular time and a particular
        bounding box.

        Args:
            start_date:     datetime for the start
            end_date:       datetime for the end
            bbox:           list of  [lonmin,latmin,lonmax,latmax]
            output_dir:     Base path to location of files
            file_type:      'grib' or 'netcdf', determines how to read the file
            forecast_flag:  weather or not to get forecast hours
            force_zone_number: UTM zone number to convert datetime to
            var_keys:       which keys to grab from smrf variables,
                            default is var_map

        Returns:
            List containing dataframe for the metadata for each node point for
            the desired variables
        """

        if start_date > end_date:
            raise ValueError('start_date before end_date')

        self.start_date = start_date
        self.end_date = end_date

        if file_type == GribFile.SUFFIX:
            self.file_loader = GribFile()
            var_map = GribFile.VAR_MAP
        elif file_type == NetCdfFile.SUFFIX:
            self.file_loader = NetCdfFile()
            var_map = NetCdfFile.VAR_MAP
        else:
            raise Exception('Unknown file type argument')

        self.file_loader.bbox = bbox

        self.log.info('Getting saved data')
        if var_map is None:
            self.log.info(
                'var_map not specified, will return default outputs'
            )

        self.force_zone_number = force_zone_number
        if output_dir is not None:
            self.output_dir = output_dir

        # filter to desired keys if specified
        if var_keys is not None:
            var_map = {key: var_map[key] for key in var_keys}

        if forecast_flag:
            # TODO: Implement forecast retrieval here
            raise NotImplementedError(
                'Getting the forecast is not implemented yet')
        else:
            self.get_data(var_map)

        metadata, dataframe = self.convert_to_dataframes(var_map)

        # manipulate data in necessary ways
        for key in dataframe.keys():
            dataframe[key].sort_index(axis=0, inplace=True)
            if key == 'air_temp':
                dataframe['air_temp'] -= 273.15
            if key == 'cloud_factor':
                dataframe['cloud_factor'] = 1 - dataframe['cloud_factor'] / 100

        return metadata, dataframe

    def get_data(self, var_map):
        """
        Get the HRRR data for set start and end date.
        Read data is stored on instance attribute.

        hours    0    1    2    3    4
                 |----|----|----|----|
        forecast
        start    fx hour
                 |----|----|----|----|
        00       01   02   03   04   05
        01            01   02   03   04
        02                 01   02   03
        03                      01   02

        """
        date = self.start_date
        data = []

        while date <= self.end_date:
            self.log.debug('Reading file for date: {}'.format(date))
            forecast_data = None

            # make sure we get a working file. This allows for six tries,
            # accounting for the fact that we start at forecast hour 1
            file_time = date
            for fx_hr in range(1, self.MAX_FORECAST_HOUR + 1):
                day_folder, file_name = FileHandler.folder_and_file(
                    file_time, fx_hr, self.file_type
                )

                try:
                    if self.file_type == GribFile.SUFFIX:
                        base_path = os.path.abspath(self.output_dir)
                        file = os.path.join(base_path, day_folder, file_name)
                        if os.path.exists(file):
                            forecast_data = self.file_loader.load(
                                file, var_map
                            )
                        else:
                            self.log.error('  No file for {}'.format(file))

                    elif self.file_type == NetCdfFile.SUFFIX:
                        file = [self.output_dir, day_folder, file_name]
                        forecast_data = self.file_loader.load(file)

                except Exception as e:
                    self.log.debug(e)
                    self.log.debug(
                        '  Could not load forecast hour {} for date {} '
                        'successfully'.format(fx_hr, date)
                    )

                if fx_hr == self.MAX_FORECAST_HOUR + 1:
                    raise IOError(
                        'Not able to find good file for {}'
                        .format(file_time.strftime('%Y-%m-%d %H:%M'))
                    )

                if forecast_data is not None:
                    data += forecast_data
                    break

            date += self.NEXT_HOUR

        try:
            self.data = xr.combine_by_coords(data)
        except Exception as e:
            self.log.debug(e)
            self.log.debug(
                '  Could not combine forecast data for given dates: {} - {}'
                    .format(self.start_date, self.end_date)
            )

    def convert_to_dataframes(self, var_map):
        """
        Convert the xarray's to dataframes to return

        Args:
            var_map: Variable map

        Returns
            Tuple of metadata and dataframe
        """
        metadata = None
        dataframe = {}

        for key, value in var_map.items():
            if self.file_type == GribFile.SUFFIX:
                df = self.data[key].to_dataframe()
            else:
                df = self.data[value].to_dataframe()
                key = value

            # convert from a row multi-index to a column multi-index
            df = df.unstack(level=[1, 2])

            # Get the metadata using the elevation variables
            if key == 'elevation':
                if self.file_type == GribFile.SUFFIX:
                    value = key

                metadata = []
                for mm in ['latitude', 'longitude', value]:
                    dftmp = df[mm].copy()
                    cols = ['grid_{}_{}'.format(x[0], x[1])
                            for x in dftmp.columns.to_flat_index()]
                    dftmp.columns = cols
                    dftmp = dftmp.iloc[0]
                    dftmp.name = mm
                    metadata.append(dftmp)

                metadata = pd.concat(metadata, axis=1)
                # it's reporting in degrees from the east
                metadata['longitude'] -= 360
                metadata = metadata.apply(
                    FileLoader.apply_utm,
                    args=(self.force_zone_number,),
                    axis=1
                )
                metadata.rename(columns={value: key}, inplace=True)

            else:
                # else this is just a normal variable
                df = df.loc[:, key]

                # make new names for the columns as grid_y_x
                cols = ['grid_{}_{}'.format(x[0], x[1])
                        for x in df.columns.to_flat_index()]
                df.columns = cols
                df.index.rename('date_time', inplace=True)

                # drop any nan values
                df.dropna(axis=1, how='all', inplace=True)
                dataframe[key] = df

        # the metadata may have more columns than the dataframes
        c = []
        for key in dataframe.keys():
            c.extend(list(dataframe[key].columns.values))

        metadata = metadata[metadata.index.isin(list(set(c)))]

        return metadata, dataframe

    @staticmethod
    def apply_utm(dataframe, force_zone_number):
        """
        Ufunc to calculate the utm from lat/lon for a series

        Args:
            dataframe: pandas series with fields latitude and longitude
            force_zone_number: default None, zone number to force to

        Returns:
            Pandas series entry with fields 'utm_x' and 'utm_y' filled
        """
        (dataframe['utm_x'], dataframe['utm_y'], *unused) = utm.from_latlon(
            dataframe.latitude, dataframe.longitude,
            force_zone_number=force_zone_number
        )

        return dataframe

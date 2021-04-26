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

    def __init__(self,
                 file_dir,
                 file_type='grib2',
                 config=None,
                 external_logger=None
                 ):
        """
        :param file_dir:        Base directory to location of files
        :param file_type:       'grib2' or 'netcdf', determines how to read
                                the files. Default: grib2
        :param config:          (Optional) Full path to a .ini file or
                                a dictionary
        :param external_logger: (Optional) Specify an existing logger instance
        """
        super().__init__(
            __name__, config=config, external_logger=external_logger
        )

        self.data = None
        self.force_zone_number = None

        self.file_type = file_type
        self.file_dir = file_dir

    @property
    def file_dir(self):
        return self._file_dir

    @file_dir.setter
    def file_dir(self, value):
        self._file_dir = value

    @property
    def file_type(self):
        return self._file_loader.SUFFIX

    @file_type.setter
    def file_type(self, value):
        if value == GribFile.SUFFIX:
            self._file_loader = GribFile(external_logger=self.log)
        elif value == NetCdfFile.SUFFIX:
            self._file_loader = NetCdfFile(external_logger=self.log)
        else:
            raise Exception('Unknown file type argument')

    @property
    def file_loader(self):
        return self._file_loader

    def get_saved_data(self,
                       start_date, end_date, bbox,
                       force_zone_number=None,
                       var_keys=None):
        """
        Get the saved data from above for a particular time and a particular
        bounding box.

        Args:
            start_date:     datetime for the start
            end_date:       datetime for the end
            bbox:           list of  [lonmin, latmin, lonmax, latmax]
            force_zone_number: UTM zone number to convert datetime to
            var_keys:       which keys to grab from smrf variables,
                            default is var_map

        Returns:
            List containing dataframe for the metadata adn for each read
            variable.
        """

        if start_date > end_date:
            raise ValueError('start_date before end_date')

        self.start_date = start_date
        self.end_date = end_date
        self.file_loader.bbox = bbox

        # filter to desired keys if specified
        if var_keys is not None:
            var_map = {key: self.file_loader.VAR_MAP[key] for key in var_keys}
        else:
            var_map = self.file_loader.VAR_MAP
            self.log.info(
                'var_map not specified, will return default outputs'
            )

        self.force_zone_number = force_zone_number

        self.log.info('Getting saved data')
        self.get_data(var_map)

        return self.convert_to_dataframes(var_map)

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
                        base_path = os.path.abspath(self.file_dir)
                        file = os.path.join(base_path, day_folder, file_name)
                        if os.path.exists(file):
                            forecast_data = self.file_loader.load(
                                file, var_map
                            )
                        else:
                            self.log.error('  No file for {}'.format(file))

                    elif self.file_type == NetCdfFile.SUFFIX:
                        file = [self.file_dir, day_folder, file_name]
                        forecast_data = self.file_loader.load(file)

                except Exception as e:
                    self.log.debug(e)
                    self.log.debug(
                        '  Could not load forecast hour {} for date {} '
                        'successfully'.format(fx_hr, date)
                    )

                if fx_hr == self.MAX_FORECAST_HOUR:
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
                    dftmp.columns = self.format_column_names(dftmp)
                    dftmp = dftmp.iloc[0]
                    dftmp.name = mm
                    metadata.append(dftmp)

                metadata = pd.concat(metadata, axis=1)
                metadata = metadata.apply(
                    FileLoader.apply_utm,
                    args=(self.force_zone_number,),
                    axis=1
                )
                metadata.rename(columns={value: key}, inplace=True)

            else:
                df = df.loc[:, key]

                df.columns = self.format_column_names(df)
                df.index.rename('date_time', inplace=True)

                df.dropna(axis=1, how='all', inplace=True)
                df.sort_index(axis=0, inplace=True)
                dataframe[key] = df

                # manipulate data in necessary ways
                if key == 'air_temp':
                    dataframe['air_temp'] -= 273.15
                if key == 'cloud_factor':
                    dataframe['cloud_factor'] = \
                        1 - dataframe['cloud_factor'] / 100

        # the metadata may have more columns than the dataframes
        c = []
        for key in dataframe.keys():
            c.extend(list(dataframe[key].columns.values))

        metadata = metadata[metadata.index.isin(list(set(c)))]

        return metadata, dataframe

    @staticmethod
    def format_column_names(dataframe):
        """
        Make new names for the columns as grid_y_x

        :param dataframe:
        :return: Array - New column names including the y and x GRIB pixel
                         index. Example: grid_0_1 for y at 0 and x at 1
        """
        return [
            'grid_{c[0]}_{c[1]}'.format(c=col)
            for col in dataframe.columns.to_flat_index()
        ]

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
        # HRRR has longitude reporting in degrees from the east
        dataframe['longitude'] -= 360

        (dataframe['utm_x'], dataframe['utm_y'], *unused) = utm.from_latlon(
            dataframe['latitude'],
            dataframe['longitude'],
            force_zone_number=force_zone_number
        )

        return dataframe

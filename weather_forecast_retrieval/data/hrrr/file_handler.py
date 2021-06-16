import pandas as pd
import re

from .grib_file import GribFile
from .netcdf_file import NetCdfFile


class FileHandler:
    """
    Class to handle downloaded HRRR files.
    """
    SINGLE_DAY_FORMAT = '%Y%m%d'
    ONE_DAY = pd.to_timedelta('1 day')

    FOLDER_NAME_BASE = 'hrrr.{}'
    FILE_NAME_BASE = 'hrrr.t{:02d}z.wrfsfcf{:02d}.{}'
    FILE_PATTERN = r'hrrr.t(\d\d)z.wrfsfcf(\d\d).grib2'

    @staticmethod
    def file_date(date, forecast_hour):
        """
        hours    0    1    2    3    4
                 |----|----|----|----|
        forecast
        start    forecast hour
                 |----|----|----|----|
        00       01   02   03   04   05
        01            01   02   03   04
        02                 01   02   03
        03                      01   02

        Args:
            date:           Datetime that the filename is created for
            forecast_hour:  Forecast hour

        Returns:
            Date and file hour of day corresponding to requested
            date and forecast_hour
        """

        date = pd.to_datetime(date)
        forecast_hour = int(forecast_hour)

        hour_of_day = int(date.hour)
        date = date.date()

        # File hour given the hour of the day and the forecast hour
        file_hour = hour_of_day - forecast_hour

        # Get previous day for the first hour of the day
        if file_hour < 0:
            date -= FileHandler.ONE_DAY
            file_hour += 24

        return date, file_hour

    @staticmethod
    def folder_name(day):
        return FileHandler.FOLDER_NAME_BASE.format(
            day.strftime(FileHandler.SINGLE_DAY_FORMAT)
        )

    @staticmethod
    def file_name(hour_of_day, forecast_hour, file_extension=None):
        if file_extension is None:
            file_extension = GribFile.SUFFIX
        elif file_extension is NetCdfFile.SUFFIX:
            file_extension = 'nc'

        return FileHandler.FILE_NAME_BASE.format(
            hour_of_day, forecast_hour, file_extension
        )

    @staticmethod
    def folder_and_file(date, forecast_hour, file_extension=None):
        """
        Get the file and folder name for a specific forecast hour of HRRR

        Args:
            date:           Datetime that the filename is created for
            forecast_hour:  Forecast hour
            file_extension: File name extension (Default: grib2)

        Returns:
            day_folder: Folder name containing the day file (hrrr.YYYYMMDD)
            file_name:  File name for the date requested
        """
        day, file_hour = FileHandler.file_date(date, forecast_hour)

        return FileHandler.folder_name(day), \
            FileHandler.file_name(file_hour, forecast_hour, file_extension)

    @staticmethod
    def folder_to_date(folder_date, file_name):
        """Given a folder and file name, get the date.

        NOTE: this will return the date for the initialize hour and
        not account for the forecast hour.

        Args:
            folder_date (str): HRRR folder name
            file_name (str): HRRR file name

        Returns:
            pd.Timestamp: Timestamp for the file
        """

        # parse the folder date
        date = pd.to_datetime(folder_date.split('.')[-1])

        # parse the file name
        res = re.search(FileHandler.FILE_PATTERN, file_name)
        hour = int(res.group(1))

        date = date.replace(hour=hour)
        return date.tz_localize(tz='UTC')

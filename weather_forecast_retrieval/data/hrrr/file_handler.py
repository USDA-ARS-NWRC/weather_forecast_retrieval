import pandas as pd


class FileHandler:
    """
    Class to handle downloaded HRRR data.
    """
    GRIB_FILE = 'grib2'
    NETCDF_FILE = 'netcdf'

    SINGLE_DAY_FORMAT = '%Y%m%d'
    ONE_DAY = pd.to_timedelta('1 day')

    FOLDER_NAME_BASE = 'hrrr.{}'
    FILE_NAME_BASE = 'hrrr.t{:02d}z.wrfsfcf{:02d}.{}'

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
            file_extension = FileHandler.GRIB_FILE
        elif file_extension is FileHandler.NETCDF_FILE:
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

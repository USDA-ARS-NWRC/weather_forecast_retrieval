import xarray as xr
from siphon.catalog import TDSCatalog

from weather_forecast_retrieval.data.hrrr.base_file import BaseFile


class NetCdfFile(BaseFile):
    SUFFIX = 'netcdf'

    # variable map to read the netcdf, the field names are those
    # converted from wgrib2 by default
    VAR_MAP = {
        'air_temp': 'TMP_2maboveground',
        'dew_point': 'DPT_2maboveground',
        'relative_humidity': 'RH_2maboveground',
        'wind_u': 'UGRD_10maboveground',
        'wind_v': 'VGRD_10maboveground',
        'precip_int': 'APCP_surface',
        'short_wave': 'DSWRF_surface',
        'elevation': 'HGT_surface',
    }

    def __init__(self, config_file=None, external_logger=None):
        super().__init__(
            __name__, config_file=config_file, external_logger=external_logger
        )

        self.main_cat = None
        self.day_cat = None

    def __del__(self):
        """
        Clean up the TDS catalog sessions when HRRR is done
        """
        # TDS catalog sessions
        if self.main_cat is not None:
            if hasattr(self.main_cat, 'session'):
                self.main_cat.session.close()
        if self.day_cat is not None:
            if hasattr(self.day_cat, 'session'):
                self.day_cat.session.close()

    def load(self, file):
        """
        Get valid HRRR data

        Args:
            file: Path of file to open

        Returns:
            Array with Xarray dataset for all variables
        """
        variable_data = []

        try:
            # instead of opening a session every time, just reuse
            if self.main_cat is None:
                self.main_cat = TDSCatalog(file[0])

            # have to ensure to change the day catalog if the day changes
            if self.day_cat is None:
                self.day_cat = TDSCatalog(
                    self.main_cat.catalog_refs[file[1]].href)
            elif self.main_cat.catalog_refs[file[1]].href != \
                    self.day_cat.catalog_url:
                # close the old session and start a new one
                if hasattr(self.day_cat, 'session'):
                    self.day_cat.session.close()
                self.day_cat = TDSCatalog(
                    self.main_cat.catalog_refs[file[1]].href)

            if len(self.day_cat.datasets) == 0:
                raise Exception(
                    ("HRRR netcdf THREDDS catalog has"
                     " no datasets for day {}").format(file[1]))

            # go through and get the file reference
            if file[2] not in self.day_cat.datasets.keys():
                raise Exception(
                    '{}/{} does not exist on THREDDS server'.format(
                        file[1], file[2]))

            d = self.day_cat.datasets[file[2]]

            self.log.info('Reading {}'.format(file[2]))
            data = xr.open_dataset(d.access_urls['OPENDAP'])

            s = data.where((data.latitude >= self.bbox[1]) &
                           (data.latitude <= self.bbox[3]) &
                           (data.longitude >= self.bbox[0] + 360) &
                           (data.longitude <= self.bbox[2] + 360),
                           drop=True)

            data.close()
            variable_data.append(s)

        except Exception as e:
            self.log.warning(e)
            variable_data = None

        return variable_data

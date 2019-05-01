import os
import datetime
import pandas as pd
import openstack
import pygrib

class HRRRStorage():
    """
    Class for managing storeage of HRRR model output files
    """
    fmt_day ='%Y%m%d'

    def __init__(self, location, storage_type, tmpdir=None):
        """
        Args:
            location:       File path or object container where hrrr data is stored
            storage_type:   either file or object
            tmpdir:         location where downloaded hrrr files will be stored temporarily
                            when using the object storage
        """
        self.location = location
        self.storage_type = storage_type
        if self.storage_type not in ['file', 'object']:
            raise Exception('storage_type is not a valid option: {}'.format(storage_type))
        self.tmpdir = tmpdir

        # connect to openstack
        if self.storage_type == 'object':
            # connect to openstack
            self.conn = openstack.connect()

    def __exit__(self):
        if self.tmpdir is not None:
            tmp_file = os.path.join(self.tmpdir, 'tmp.grib2')
            if os.path.isfile(tmp_file):
                os.remove(tmp_file)

    def cleanup(self):
        self.__exit__()

    def get_hrrr_file_date(self, fp, fx=False):
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

    def hrrr_name_finder(self, date, fx_hr = 0):
        """
        Find the file pointer for a hrrr file with a specific forecast hour

        Args:
            base_path:  The base HRRR directory. For ./data/forecasts/hrrr/hrrr.20180203/...
                        the base_path is ./forecasts/hrrr/
            date:       datetime that the file is used for
            fx_hr:      forecast hour
        Returns:
            fp:         string of absolute path to the file

        """

        date = pd.to_datetime(date)
        fx_hr = int(fx_hr)

        day = date.date()
        hr = int(date.hour)

        # find the new base hour given the date and forecast hour
        new_hr = hr - fx_hr

        # if we've dropped back a day, fix logic to reflect that
        if new_hr < 0:
            day = day - pd.to_timedelta('1 day')
            new_hr = new_hr + 24

        if self.storage_type == 'file':
            return self.hrrr_file_name_finder(new_hr, fx_hr, day)
        elif self.storage_type == 'object':
            return self.hrrr_object_name_finer(new_hr, fx_hr, day)
        else:
            raise Exception('Storage type must be "file" or "object"')

    def hrrr_file_name_finder(self, new_hr, fx_hr, day):
        """
        Find the file pointer for a hrrr file with a specific forecast hour

        Args:
            new_hr:     the base hour of the hrrr file
            fx_hr:      the forecast hour of the hrrr file
            day:        the date of the hrrr folder
        Returns:
            fp:         string of absolute path to the file

        """

        base_path = os.path.abspath(self.location)

        # create new path
        fp = os.path.join(base_path, 'hrrr.{}'.format(day.strftime(self.fmt_day)),
                          'hrrr.t{:02d}z.wrfsfcf{:02d}.grib2'.format(new_hr, fx_hr))

        return fp

    def hrrr_object_name_finer(self, new_hr, fx_hr, day):
        """
        Find the file pointer for a hrrr file with a specific forecast hour.
        In order to use this functionality, make sure your clouds.yaml file
        is in ~/.config/openstack/ and add your openstack password to the file

        Args:
            new_hr:     the base hour of the hrrr file
            fx_hr:      the forecast hour of the hrrr file
            day:        the date of the hrrr folder
        Returns:
            fp:         string of absolute path to the file that was just downloaded
        """
        if self.tmpdir is None:
            raise Exception('Need a tmpdir to get HRRR files from openstack')

        # find the file name
        object_name = os.path.join('hrrr.{}'.format(day.strftime(self.fmt_day)),
                                   'hrrr.t{:02d}z.wrfsfcf{:02d}.grib2'.format(new_hr, fx_hr))
        container = self.location
        # make sure the object is in the container
        container_objs = self.conn.object_store.objects(container)
        c_obj_names = [cobj.name for cobj in container_objs]
        # Return False if we can't find the file
        # Returning False will make us look for a new file
        if object_name not in c_obj_names:
            print('Cannot find object {} in container {}'.format(object_name,
                                                                 container))
            print('Will try another file')
            return False

        # get the object data and store it in a file
        tmp_file = os.path.join(self.tmpdir, 'tmp.grib2')
        data = self.conn.object_store.download_object(object_name, container=container)
        with open(tmp_file, 'wb') as fp:
            fp.write(data)

        return tmp_file

    def upload_hrrr_file(self, container, object_name, fp):
        """
        Take a local HRRR grib2 file and upload it to an openstack container store

        Args:
            container:      Name of openstack container
            object_name:    Object name for storage in the container
            fp:             path to local file

        """
        # upload file
        with open(fp, 'rb') as dt:
            up =  self.conn.object_store.upload_object(container, object_name, data=dt,
                                                       generate_checksums=True)

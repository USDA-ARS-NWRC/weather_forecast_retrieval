import argparse
import logging
import os
import shutil
import subprocess

import pandas as pd

from weather_forecast_retrieval.data.hrrr import FileHandler


class HRRRPreprocessor():
    VARIABLES = [
        'TMP:2 m',
        'RH:2 m',
        'UGRD:10 m',
        'VGRD:10 m',
        'APCP:surface',
        'DSWRF:surface',
        'HGT:surface',
        'TCDC:entire atmosphere'
    ]

    def __init__(self, hrrr_dir, start_date, end_date, output_dir,
                 bbox, forecast_hr, ncpu=0, verbose=False):

        log_level = logging.INFO
        if verbose:
            log_level = logging.DEBUG

        logging.basicConfig(level=log_level)
        self._logger = logging.getLogger('HRRRPreprocessor')
        self.verbose = verbose

        if shutil.which('wgrib2') is None:
            raise Exception('wgrib2 is not installed')

        # output directory for hrrr
        self.hrrr_dir = hrrr_dir

        # output directory for cropped hrrr files
        self.output_dir = output_dir
        os.makedirs(self.output_dir, exist_ok=True)

        # bounding box
        self.lonw = bbox[0]
        self.lone = bbox[1]
        self.lats = bbox[2]
        self.latn = bbox[3]

        # start and end date
        self.start_date = pd.to_datetime(start_date)
        self.end_date = pd.to_datetime(end_date)
        self.date_times = pd.date_range(
            self.start_date, self.end_date, freq='H')

        # forecast number, only do one at a time so multiple can run at once
        self.forecast_hr = forecast_hr

        # ncpu arg for wgrib2, 0 will default to all available cpu's
        self.ncpu = '' if ncpu == 0 else '-ncpu {}'.format(ncpu)

        self._logger.info('HRRR directory: {}'.format(self.hrrr_dir))
        self._logger.info('Cropped HRRR directory: {}'.format(self.output_dir))
        self._logger.info('Process files between {} and {}'.format(
            self.start_date, self.end_date
        ))
        self._logger.info(
            '{} hours will be processed'.format(len(self.date_times)))
        self._logger.info('Forecast hour: {}'.format(self.forecast_hr))
        self._logger.info('Number of cpu argument: {}'.format(self.ncpu))

    @property
    def variable_match(self):
        return '|'.join(self.VARIABLES)

    def check_for_good_file(self, file_name):

        check_action = 'wgrib2 {}'.format(file_name)
        status, output = self.call_wgrib2(check_action)

        bad_flag = False
        if status != 0:
            bad_flag = True
        else:
            output = ''.join(output)
            for variable in self.VARIABLES:
                if variable not in output:
                    self._logger.warning('Variable {} not in file'.format(variable))
                    bad_flag = True

                    if not self.verbose and bad_flag:
                        break

        if bad_flag:
            self._logger.warning('Removing {}'.format(file_name))
            os.remove(file_name)

    def call_wgrib2(self, action):
        """Execute a wgrib2 command
        Arguments:
            action {str} -- command for wgrib2 to execute
            logger {logger} -- logger instance
        Returns:
            Boolean -- True if call succeeds
        """

        # run command line using Popen
        self._logger.debug('Running "{}"'.format(action))

        with subprocess.Popen(
            action,
            shell=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            universal_newlines=True
        ) as s:

            # stream the output of wgrib2 to the logger
            return_code = s.wait()
            output = []
            if return_code:
                for line in s.stdout:
                    self._logger.warning(line.rstrip())
                    output.append(line.rstrip())
                self._logger.warning(
                    "An error occured while running wgrib2 action")
            else:
                for line in s.stdout:
                    self._logger.debug(line.rstrip())
                    output.append(line.rstrip())

            return return_code, output

    def run(self):

        for date_time in self.date_times:
            self._logger.info('Processing date: {}'.format(date_time))

            # get the file and path's
            hrrr_day_dir = FileHandler.folder_name(date_time)
            hrrr_file_name = FileHandler.file_name(
                date_time.hour, self.forecast_hr
            )
            hrrr_abs_file_path = os.path.join(
                self.hrrr_dir,
                hrrr_day_dir,
                hrrr_file_name
            )

            new_hrrr_path = os.path.join(
                self.output_dir,
                hrrr_day_dir
            )
            os.makedirs(new_hrrr_path, exist_ok=True)
            new_hrrr_file = os.path.join(new_hrrr_path, hrrr_file_name)

            # one command to crop then extract the variables
            pipe_action = """wgrib2 {} {} -small_grib {}:{} {}:{} - | """ \
                """wgrib2 - -match '{}' -GRIB {}""".format(
                    hrrr_abs_file_path,
                    self.ncpu,
                    self.lonw,
                    self.lone,
                    self.lats,
                    self.latn,
                    self.variable_match,
                    new_hrrr_file
                )

            self.call_wgrib2(pipe_action)

            # Check that the file has been created and is not empty or corrupt
            self.check_for_good_file(new_hrrr_file)


def cli():
    """
    Command line tool to preprocess HRRR into smaller files
    """

    parser = argparse.ArgumentParser(
        description="""Crop HRRR files by a bounding box and """
                    """extract only the necessary surface variables """
                    """for running with AWSM. \n\nExample command:\n"""
                    """$ hrrr_preprocessor -s '2019-10-01 00:00' """
                    """-e '2019-10-01 02:00' -f 0 --bbox="-119,-118,37,38" """
                    """-o /path/to/output --verbose """
                    """/path/to/hrrr""",
        formatter_class=argparse.RawDescriptionHelpFormatter)

    parser.add_argument('hrrr_dir', metavar='hrrr_dir', type=str,
                        help='Directory of HRRR files to use as input')

    parser.add_argument('-o', '--output_dir', dest='output_dir',
                        type=str, required=True,
                        help='Directory to write cropped HRRR files to')

    parser.add_argument('-s', '--start', dest='start_date',
                        required=True, help='Start date')

    parser.add_argument('-e', '--end', dest='end_date',
                        required=True, help='End date')

    parser.add_argument('-f', '--forecast_hr', dest='forecast_hr', type=int,
                        required=True, help='Forecast hour')

    parser.add_argument('-n', '--ncpu', dest='ncpu', type=int, default=0,
                        help='Number of CPUs for wgrib2, 0 (default) will use all available')

    parser.add_argument('--bbox', dest='bbox',
                        type=lambda s: [i for i in s.split(',')],
                        required=True,
                        help="""Bounding box as delimited string """
                        """--bbox='longitude left, longitude right, latitude bottom, """
                        """latitude top'""")

    parser.add_argument("--verbose", help="increase logging verbosity",
                        action="store_true")

    args = parser.parse_args()

    HRRRPreprocessor(**vars(args)).run()

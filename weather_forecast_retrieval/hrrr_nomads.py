from datetime import datetime, timedelta
import argparse

from weather_forecast_retrieval.data.hrrr import HttpRetrieval
from weather_forecast_retrieval.hrrr_preprocessor import HRRRPreprocessor


class HRRRNOMADS():
    """Helper class to aid in the interaction with downloading HRRR
    data from NOMADS.

    TODO: Check if file is already downloaded
    """

    def __init__(self, output_dir, num_requests=2, overwrite=False, verbose=False) -> None:

        self.output_dir = output_dir
        self.num_requests = num_requests
        self.overwrite = overwrite
        self.verbose = verbose

        self.http_retrieval = HttpRetrieval(overwrite=overwrite)
        self.http_retrieval.output_dir = output_dir

    def set_dates(self, start_date, end_date, forecast_hrs):

        self.start_date = start_date
        self.end_date = end_date
        self.forecast_hrs = forecast_hrs

    def date_range(self, start_date, end_date, forecast_hrs=None):
        """Get files within a date range.

        Args:
            start_date (str): start date of period
            end_date (str): end date of period
            forecast_hrs (list, optional): Forecast hours to download.
                Defaults to None or download all hours.

        Returns:
            list: List of response objects
        """

        self.set_dates(start_date, end_date, forecast_hrs)
        return self.http_retrieval.fetch_by_date(start_date, end_date, forecast_hrs)

    def latest(self, latest=3, forecast_hrs=None):
        """Download files for the lastest hours.

        Args:
            latest (int, optional): Number of hours to download. Defaults to 3.
            forecast_hrs (list, optional): Forecast hours to download.
                Defaults to None or download all hours.

        Returns:
            list: List of response objects
        """

        start_date = datetime.utcnow().replace(microsecond=0)
        end_date = start_date + timedelta(hours=latest)
        self.set_dates(start_date, end_date, forecast_hrs)

        return self.http_retrieval.fetch_by_date(start_date, end_date, forecast_hrs)

    def preprocessing(self, bbox, output_path):
        """Preprocess the downloaded files by cropping and extracting to the
        output_path

        Args:
            bbox (list): Bounding box [lon W, lon E, lat S, lat N]
            output_path (str): directory to put cropped files

        Raises:
            ValueError: forecast_hrs must be set explicitly
        """

        if self.forecast_hrs is None:
            raise ValueError('Must specify forecast hours when preprocessing')

        for fx_hr in self.forecast_hrs:

            HRRRPreprocessor(
                self.output_dir,
                self.start_date,
                self.end_date,
                output_path,
                bbox,
                fx_hr,
                verbose=self.verbose
            ).run()


def main(**kwargs):
    hn = HRRRNOMADS(kwargs['output_dir'], kwargs['num_requests'], kwargs['verbose'])

    if 'start_date' in kwargs.keys() and 'end_date' in kwargs.keys():
        results = hn.date_range(kwargs['start_date'],
                                kwargs['end_date'],
                                kwargs['forecast_hrs'])
    else:
        results = hn.latest(kwargs['latest'],
                            kwargs['forecast_hrs'])

    if kwargs['bbox'] and kwargs['output_path']:
        hn.preprocessing(kwargs['bbox'], kwargs['output_path'])

    return results


def cli():
    """
    Command line tool to download files from NOMADS and preprocess those files
    """

    parser = argparse.ArgumentParser(
        description="Crop HRRR files by a bounding box and "
                    "extract only the necessary surface variables "
                    "for running with AWSM. \n\nExample command:\n"
                    "$ hrrr_preprocessor -s '2019-10-01 00:00' "
                    "-e '2019-10-01 02:00' -f 0 --bbox=\"-119,-118,37,38\" "
                    "-o /path/to/output --verbose "
                    "/path/to/hrrr"
    )

    parser.add_argument('-o', '--output_dir',
                        dest='output_dir',
                        type=str,
                        required=True,
                        help='Directory to download HRRR files to')

    parser.add_argument('-n', '--num_requests',
                        dest='num_requests',
                        default=2,
                        help='Number of concurrent requests, default 2')

    parser.add_argument('-s', '--start',
                        dest='start_date',
                        required=False,
                        help='Start date')

    parser.add_argument('-e', '--end',
                        dest='end_date',
                        required=False,
                        help='End date')

    parser.add_argument('-l', '--latest',
                        dest='latest',
                        type=float,
                        default=3,
                        help='Latest number of hours to download, defaults 3 hours')

    parser.add_argument('-f', '--forecast_hrs',
                        dest='forecast_hrs',
                        type=lambda s: [i for i in s.split(',')],
                        default=None,
                        help='Forecast hours, comma seperated list')

    # parser.add_argument('-n', '--ncpu', dest='ncpu', type=int, default=0,
    #                     help='Number of CPUs for wgrib2, 0 (default) will use '
    #                          'all available')

    parser.add_argument('--bbox',
                        dest='bbox',
                        type=lambda s: [i for i in s.split(',')],
                        required=False,
                        help="Bounding box as delimited string "
                        "--bbox='longitude left, longitude right, "
                             "latitude bottom, latitude top'")

    parser.add_argument('-p', '--preprocess_path',
                        dest='output_path',
                        type=str,
                        required=False,
                        help='Directory to write preprocessed HRRR files')

    parser.add_argument("--verbose",
                        help="increase logging verbosity",
                        action="store_true")

    args = parser.parse_args()
    main(**vars(args))

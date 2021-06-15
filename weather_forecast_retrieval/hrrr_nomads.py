from datetime import datetime, timedelta

from weather_forecast_retrieval.data.hrrr import HttpRetrieval


class HRRRNOMADS():
    """Helper class to aid in the interaction with downloading HRRR
    data from NOMADS.

    - start/end date
    - latest X hours
    - forecast hours
    - output directory
    - num requests
    - verbosity
    """

    def __init__(self, output_dir, num_requests=2, verbose=False) -> None:
        self.output_dir = output_dir
        self.num_requests = num_requests
        self.verbose = verbose

        self.http_retrieval = HttpRetrieval()
        self.http_retrieval.output_dir = output_dir

    def date_range(self, start_date, end_date, forecast_hrs=None):
        return self.http_retrieval.fetch_by_date(start_date, end_date, forecast_hrs)

    def latest(self, latest=3, forecast_hrs=None):
        start_date = datetime.utcnow().replace(microsecond=0)
        end_date = start_date + timedelta(hours=latest)

        return self.http_retrieval.fetch_by_date(start_date, end_date, forecast_hrs)

    def preprocessing(self, bbox):
        pass

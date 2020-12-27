import os

import numpy as np
import pandas as pd

from tests.RME_test_case import RMETestCase
from weather_forecast_retrieval import hrrr


def compare_gold(v_name, gold_dir, test_df):
    """
    Compares two csv files to and determines if they are the same.

    Args:
        v_name: Name with in the file contains
        gold_dir: Directory containing gold standard results
        test_df: Data frame containing test results to be compared
    Returns:
        Boolean: Whether the two images were the same
    """

    # read in the gold standard
    fp1 = os.path.join(gold_dir, v_name+'_data.csv')
    dfgold = pd.read_csv(
        fp1, 'r', delimiter=',', parse_dates=['date_time'], dtype=np.float32
    )
    dfgold.set_index('date_time', inplace=True)

    result = np.allclose(test_df.values, dfgold.values, atol=0)

    return result


class TestHRRR(RMETestCase):
    def testHRRRGribLoad(self):
        """
        Load HRRR data from grib files
        """

        # get the data
        metadata, data = hrrr.HRRR().get_saved_data(
            self.START_DATE,
            self.END_DATE,
            self.BBOX,
            file_type='grib2',
            output_dir=self.hrrr_dir.as_posix(),
            force_zone_number=self.UTM_ZONE_NUMBER)

        df = pd.read_csv(self.gold_dir.joinpath('metadata.csv').as_posix())
        df.set_index('grid', inplace=True)

        self.assertTrue(
            np.allclose(df.values, metadata[df.columns].values, atol=0)
        )

        # compare with the gold standard
        for k, df in data.items():
            status = compare_gold(k, self.gold_dir.as_posix(), df)
            self.assertTrue(status)

        self.assertIsNotNone(metadata)

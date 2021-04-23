import numpy as np
import pandas as pd

from tests.RME import RMETestCase
from weather_forecast_retrieval.data.hrrr import FileLoader


def compare_gold(v_name, gold_dir, test_df):
    """
    Compare against gold HRRR data stored as csv in basin directory.

    Args:
        v_name: Variable to compare that also serves as the file name to load.
        gold_dir: Directory containing gold standard results
        test_df: Data frame containing test results to be compared
    """

    df_gold = pd.read_csv(
        gold_dir.joinpath(v_name+'_data.csv').as_posix(),
        header=0,
        delimiter=',',
        parse_dates=['date_time'],
        index_col='date_time',
        dtype=np.float32,
    )

    np.testing.assert_allclose(test_df.values, df_gold.values, rtol=1e-4)


class TestHRRR(RMETestCase):
    def testHRRRGribLoad(self):
        """
        Load HRRR data from multiple grib files
        """
        metadata, data = FileLoader().get_saved_data(
            self.START_DATE,
            self.END_DATE,
            self.BBOX,
            file_type='grib2',
            output_dir=self.hrrr_dir.as_posix(),
            force_zone_number=self.UTM_ZONE_NUMBER
        )

        df = pd.read_csv(
            self.gold_dir.joinpath('metadata.csv').as_posix(),
            header=0,
            index_col='grid'
        )

        self.assertIsNotNone(metadata)
        np.testing.assert_allclose(
            df.values, metadata[df.columns].values, rtol=1e-4
        )

        [compare_gold(k, self.gold_dir, df) for k, df in data.items()]

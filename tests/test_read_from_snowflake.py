import unittest
import pandas as pd
import cloudy_warehouses


class TestWriteSnowflake(unittest.TestCase):

    def test_read_snowflake(self):
        df = pd.read_snowflake(
            database='TEST_DB',
            schema='PANDAS_CLOUDY_TEST',
            table='TO_SNOWFLAKE_TEST'
        )

        self.assertIsInstance(
            df,
            pd.DataFrame
        )
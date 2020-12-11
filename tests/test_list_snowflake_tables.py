import unittest
import pandas as pd
import cloudy_warehouses


class TestListSnowflake(unittest.TestCase):

    def test_list_snowflake(self):
        df = pd.list_snowflake_tables(
            database='TEST_DB'
        )

        self.assertIsInstance(
            df,
            pd.DataFrame
        )
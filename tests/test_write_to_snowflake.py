import unittest
import pandas as pd
import cloudy_warehouses


class TestWriteSnowflake(unittest.TestCase):

    def test_write_snowflake(self):

        df = pd.DataFrame.from_dict({'V': ['B', 'O', 'O', 'M']})

        expected_return_statement = 'successfully wrote to Snowflake Table'

        return_statement = df.cloudy_warehouses.write_snowflake(
            database='TEST_DB',
            schema='PANDAS_CLOUDY_TEST',
            table_name='TO_SNOWFLAKE_TEST'
        )

        self.assertEqual(
            expected_return_statement,
            return_statement
        )
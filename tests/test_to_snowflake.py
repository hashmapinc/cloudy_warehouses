import unittest
import pandas as pd
import cloudy_warehouses


class TestWriteSnowflake(unittest.TestCase):

    def test_write_snowflake(self):

        df = pd.DataFrame.from_dict({'COL_1': ['hello', 'there'], 'COL_2': [10, 1000]})

        expected_return_statement = 'successfully wrote to Snowflake Table'

        return_statement = df.cloudy_warehouses.write_snowflake(
            database='TEST_DB',
            schema='PANDAS_CLOUDY_TEST',
            table='TO_SNOWFLAKE_TEST'
        )

        self.assertEqual(
            expected_return_statement,
            return_statement
        )

    def test_create_snowflake(self):

        df = pd.DataFrame.from_dict({'COL_1': ['hello', 'there'], 'COL_2': [10, 20], 'COL_3': [10, 20]})

        expected_return_statement = 'successfully created and wrote to Snowflake Table'

        return_statement = df.cloudy_warehouses.create_snowflake(
            database='TEST_DB',
            schema='PANDAS_CLOUDY_TEST',
            table='CREATE_SNOWFLAKE_TEST'
        )

        self.assertEqual(
            expected_return_statement,
            return_statement
        )

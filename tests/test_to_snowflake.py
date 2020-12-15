import unittest
import pandas as pd
import cloudy_warehouses


class TestWriteSnowflake(unittest.TestCase):
    """unittests for SnowflakeWriter"""

    def test_write_snowflake(self):

        df = pd.DataFrame.from_dict({'COL_1': ['hello', 'there'], 'COL_2': [10, 1000]})

        return_statement = df.cloudy_warehouses.write_snowflake(
            database='TEST_DB',
            schema='PANDAS_CLOUDY_TEST',
            table='TO_SNOWFLAKE_TEST',
            sf_username='a name',
            sf_password='a pass',
            sf_account='hashmap'
        )

        self.assertEqual(return_statement, False)

    def test_create_snowflake(self):

        df = pd.DataFrame.from_dict({'COL_1': ['hello', 'there'], 'COL_2': [10, 20], 'COL_3': [10, 20]})

        return_statement = df.cloudy_warehouses.create_snowflake(
            database='TEST_DB',
            schema='PANDAS_CLOUDY_TEST',
            table='CREATE_SNOWFLAKE_TEST'
        )

        self.assertEqual(return_statement, True)


import unittest
import pandas as pd
import cloudy_warehouses


class TestWriteSnowflake(unittest.TestCase):

    def test_clone_snowflake(self):
        return_statement = pd.clone_snowflake(
            database='TEST_DB',
            schema='PANDAS_CLOUDY_TEST',
            new_table='COPY_SNOWFLAKE_TEST',
            source_table='CREATE_SNOWFLAKE_TEST'
        )

        self.assertEqual(return_statement, "successfully created a Snowflake Table copy")

    def test_clone_empty_snowflake(self):
        return_statement = pd.clone_empty_snowflake(
            database='TEST_DB',
            schema='PANDAS_CLOUDY_TEST',
            new_table='CLONE_EMPTY_SNOWFLAKE_TEST',
            source_table='CREATE_SNOWFLAKE_TEST'
        )

        self.assertEqual(return_statement, "successfully created an empty Snowflake Table")
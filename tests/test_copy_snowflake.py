import unittest
import pandas as pd
import cloudy_warehouses


class TestCopySnowflake(unittest.TestCase):
    """unittests for SnowflakeCopier"""

    # tests for successful clone
    def test_clone_snowflake(self):
        return_statement = pd.clone_snowflake(
            database='TEST_DB',
            schema='PANDAS_CLOUDY_TEST',
            new_table='COPY_SNOWFLAKE_TEST',
            source_table='TABLES',
            source_schema='HC_TESTING'
        )

        self.assertEqual(return_statement, True)

    # tests for unsuccessful clone
    def test_clone_snowflake2(self):
        return_statement = pd.clone_snowflake(
            database='TEST_DB',
            schema='PANDAS_CLOUDY_TEST',
            new_table='COPY_SNOWFLAKE_TEST',
            source_table='TABLES',
            source_database='TEST_DB'
        )

        self.assertEqual(return_statement, False)

    # tests for successful clone empty
    def test_clone_empty_snowflake(self):
        return_statement = pd.clone_empty_snowflake(
            database='TEST_DB',
            schema='PANDAS_CLOUDY_TEST',
            new_table='CLONE_EMPTY_SNOWFLAKE_TEST',
            source_table='CREATE_SNOWFLAKE_TEST'
        )

        self.assertEqual(return_statement, True)

    # tests for unsuccessful clone empty
    def test_clone_empty_snowflake2(self):
        return_statement = pd.clone_empty_snowflake(
            database='TEST_DB',
            schema='PANDAS_CLOUDY_TEST',
            new_table='CLONE_EMPTY_SNOWFLAKE_TEST',
            source_table='CREATE_SNOWFLAKE_TEST',
            source_database='TEST_DB'
        )

        self.assertEqual(return_statement, False)

import unittest
import mock
import pandas as pd
import cloudy_warehouses


class TestCopySnowflake(unittest.TestCase):
    """Unittests for SnowflakeCopier."""

    def test_clone_snowflake(self):

        # test for success
        mock_clone_snowflake = mock.create_autospec(pd.clone_snowflake, return_value=True)

        self.assertEqual(mock_clone_snowflake(database='a database', schema='a schema',
                                              new_table='a table', source_table='source table',
                                              source_schema='source schema'), True)

        mock_clone_snowflake.assert_called_with(database='a database', schema='a schema',
                                                new_table='a table', source_table='source table',
                                                source_schema='source schema')

        # test for failure
        mock_clone_snowflake = mock.create_autospec(pd.clone_snowflake, return_value=False)

        self.assertEqual(mock_clone_snowflake(database='a database', schema='a schema',
                                              new_table='a table', source_table='source table',
                                              source_database='source database'), False)

        mock_clone_snowflake.assert_called_with(database='a database', schema='a schema',
                                                new_table='a table', source_table='source table',
                                                source_database='source database')

    def test_clone_empty_snowflake(self):

        # test for success
        mock_clone_snowflake = mock.create_autospec(pd.clone_empty_snowflake, return_value=True)

        self.assertEqual(mock_clone_snowflake(database='a database', schema='a schema',
                                              new_table='a table', source_table='source table'), True)

        mock_clone_snowflake.assert_called_with(database='a database', schema='a schema',
                                                new_table='a table', source_table='source table')

        # test for failure
        mock_clone_snowflake = mock.create_autospec(pd.clone_empty_snowflake, return_value=False)

        self.assertEqual(mock_clone_snowflake(database='a database', schema='a schema',
                                              new_table='a table', source_table='source table',
                                              source_database='source database'), False)

        mock_clone_snowflake.assert_called_with(database='a database', schema='a schema',
                                                new_table='a table', source_table='source table',
                                                source_database='source database')


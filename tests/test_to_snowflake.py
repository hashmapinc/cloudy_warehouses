import unittest
import mock
import pandas as pd
import cloudy_warehouses


class TestWriteSnowflake(unittest.TestCase):
    """Unittests for SnowflakeWriter."""

    def test_write_snowflake(self):

        df = pd.DataFrame.from_dict({'COL_1': ['hello', 'there'], 'COL_2': [10, 1000]})

        mock_write_snowflake = mock.create_autospec(df.cloudy_warehouses.write_snowflake, return_value=True)

        self.assertEqual(mock_write_snowflake(database='a database', schema='a schema', table='a table',
                                              sf_username='a name', sf_password='a pass', sf_account='an account'),
                         True)

        mock_write_snowflake.assert_called_with(database='a database', schema='a schema', table='a table',
                                                sf_username='a name', sf_password='a pass', sf_account='an account')

    def test_create_snowflake(self):

        df = pd.DataFrame.from_dict({'COL_1': ['hello', 'there'], 'COL_2': [10, 20], 'COL_3': [10, 20]})

        mock_create_snowflake = mock.create_autospec(df.cloudy_warehouses.create_snowflake, return_value=True)

        self.assertEqual(mock_create_snowflake(database='a database', schema='a schema', table='a table'), True)

        mock_create_snowflake.assert_called_with(database='a database', schema='a schema', table='a table')
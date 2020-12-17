import unittest
import mock
import pandas as pd
import cloudy_warehouses


class TestReadSnowflake(unittest.TestCase):
    """Unittests for SnowflakeReader."""

    def test_read_snowflake(self):

        mock_read_snowflake = mock.create_autospec(pd.read_snowflake,
                                                   return_value=pd.DataFrame.from_dict({
                                                       'COL_1': ['hello', 'there'],
                                                       'COL_2': [10, 1000]
                                                   })
                                                   )

        self.assertIsInstance(mock_read_snowflake(database='a database', schema='a schema', table='a table'),
                              pd.DataFrame)

        mock_read_snowflake.assert_called_with(database='a database', schema='a schema', table='a table')

    def test_list_snowflake(self):

        mock_list_snowflake = mock.create_autospec(pd.list_snowflake_tables,
                                                   return_value=pd.DataFrame.from_dict({
                                                       'COL_1': ['hello', 'there'],
                                                       'COL_2': [10, 1000]
                                                   })
                                                   )

        self.assertIsInstance(mock_list_snowflake(database='a database'), pd.DataFrame)

        mock_list_snowflake.assert_called_with(database='a database')

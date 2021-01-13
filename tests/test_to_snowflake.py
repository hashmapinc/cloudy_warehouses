import unittest
import mock
import pandas as pd
import cloudy_warehouses


class TestWriteSnowflake(unittest.TestCase):
    """Unittests for SnowflakeWriter."""

    def test_write_snowflake(self):

        df = pd.DataFrame.from_dict({'COL_1': ['hello', 'there'], 'COL_2': ['general', 'hello']})

        mock_write_snowflake = mock.create_autospec(df.cloudy_warehouses.write_snowflake, return_value=True)

        self.assertEqual(mock_write_snowflake(database='a database', schema='a schema', table='a table',
                                              username='a name', password='a pass', account='an account',
                                              warehouse='warehouse', role='role'),
                         True)

        mock_write_snowflake.assert_called_with(database='a database', schema='a schema', table='a table',
                                                username='a name', password='a pass', account='an account',
                                                warehouse='warehouse', role='role')
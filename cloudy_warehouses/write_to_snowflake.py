import pandas as pd
from snowflake.connector.pandas_tools import write_pandas
from cloudy_warehouses.snowflake_objects.snowflake_object import SnowflakeObject


# Writer Object
@pd.api.extensions.register_dataframe_accessor("cloudy_warehouses")
class SnowflakeWriter(SnowflakeObject):

    write_success = False

    def __init__(self, df: pd.DataFrame):
        self.df = df

    # Uploads data from a pandas dataframe to an existing Snowflake table
    def write_snowflake(self, database: str, schema: str, table: str, sf_username: str=None, sf_password: str=None, sf_account: str=None):

        # calls method to configure Snowflake credentials
        sf_credentials = self.configure_credentials(
            sf_username = sf_username,
            sf_password = sf_password,
            sf_account = sf_account
            )

        # calls method to connect to Snowflake using the sf_credentials variable
        connection = self.get_snowflake_connection(
            user = sf_credentials['user'],
            pswd = sf_credentials['pass'],
            acct = sf_credentials['acct'],
            database = database,
            schema = schema
            )
        
        # calls method to write data in a pandas dataframe to an existing Snowflake table
        self.write_success = self.write_pandas_dataframe(
                    connection = connection,
                    df = self.df,
                    table = table
                    )

        if self.write_success:
            return "successfully wrote to Snowflake Table"
        
        else:
            return "not successful"

    # writes data from a pandas dataframe to a Snowflake table
    def write_pandas_dataframe(self, connection, df, table):

        success, nchunks, nrows, _ = write_pandas(connection, df, table)

        return success

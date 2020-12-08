import pandas as pd
from snowflake.connector.pandas_tools import write_pandas
from snowflake_connector import get_snowflake_connection
from configure_snowflake import configure_credentials


# Writer Object
@pd.api.extensions.register_dataframe_accessor("cloudy_warehouses")
class SnowflakeWriter:

    def __init__(self, df: pd.DataFrame):
            self.df = df

    # Uploads data from a pandas dataframe to an existing Snowflake table
    def write(self, database: str, schema: str, table_name: str, sf_username: str=None, sf_password: str=None, sf_account: str=None):

        # calls function to configure Snowflake credentials
        sf_credentials = configure_credentials(
            sf_username = sf_username,
            sf_password = sf_password,
            sf_account = sf_account
            )

        # calls function to connect to Snowflake using the sf_credentials variable
        connection = get_snowflake_connection(
            user = sf_credentials['user'],
            pswd = sf_credentials['pass'],
            acct = sf_credentials['acct'],
            database = database,
            schema = schema
            )
        
         # calls function to write data in a pandas dataframe to an existing Snowflake table
        write_pandas_dataframe(
            connection = connection,
            df = self.df,
            database = database,
            schema = schema,
            table_name = table_name
            )

        return print("successfully wrote to Snowflake Table")


# writes data from a pandas dataframe to a Snowflake table
def write_pandas_dataframe(connection, df, database, schema, table_name):
    success, nchunks, nrows, _ = write_pandas(connection, df, table_name)

    return success
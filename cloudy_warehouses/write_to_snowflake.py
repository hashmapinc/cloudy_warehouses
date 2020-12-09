import pandas as pd
import os, yaml
from snowflake import connector
from snowflake.connector.pandas_tools import write_pandas


# Writer Object
@pd.api.extensions.register_dataframe_accessor("cloudy_warehouses")
class SnowflakeWriter:

    write_success = False

    def __init__(self, df: pd.DataFrame):
            self.df = df


    # Uploads data from a pandas dataframe to an existing Snowflake table
    def write(self, database: str, schema: str, table_name: str, sf_username: str=None, sf_password: str=None, sf_account: str=None):

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
                    database = database,
                    schema = schema,
                    table_name = table_name
                    )

        if self.write_success:
            return print("successfully wrote to Snowflake Table")
        
        else:
            return print("not successful")


    # configures Snowflake credentials
    def configure_credentials(self, sf_username: str=None, sf_password: str=None, sf_account: str=None):
       
        # determines where to look for Snowflake credentials
        if not sf_username or not sf_password or not sf_account:
            # Path to Snowflake credentials file
            #Add Cloudy Home Path
            __profile_path: str = os.path.join(os.getenv("HOME"), '.cloudy_warehouses/configuration_profiles.yml')

            with open(__profile_path, 'r') as stream:
                sf_credentials = yaml.safe_load(stream)['profiles']['snowflake']
        else:
            # uses passed in variables as Snowflake credentials
            sf_credentials = {
                'user': sf_username, 
                'pass': sf_password, 
                'acct': sf_account
                }
        
        return sf_credentials
    

    # establishes a connection with snowflake
    def get_snowflake_connection(self, user, pswd, acct, database=None, schema=None):

        connection = connector.connect(
            user = user,
            password = pswd,
            account = acct,
            database = database,
            schema = schema
        )

        return connection


    # writes data from a pandas dataframe to a Snowflake table
    def write_pandas_dataframe(self, connection, df, database, schema, table_name):

        success, nchunks, nrows, _ = write_pandas(connection, df, table_name)

        return success

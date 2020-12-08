import pandas as pd
from snowflake_connector import get_snowflake_connection
from configure_snowflake import configure_credentials


# Reads from Snowflake and returns pandas dataframe
class SnowflakeReader:


    def read(database: str, schema: str, table: str, sf_username: str=None, sf_password: str=None, sf_account: str=None):

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
            acct = sf_credentials['acct']
            )
        
        # calls function to return data in a Snowflake table as a pandas dataframe
        pandas_dataframe = get_pandas_dataframe(
            connection = connection,
            database = database,
            schema = schema,
            table = table
            )

        return pandas_dataframe


# reads data from a Snowflake table as a pandas dataframe
def get_pandas_dataframe(connection, database, schema, table):
    try:
        cursor = connection.cursor()
        cursor.execute(f'select * from {database}.{schema}.{table}')
        df = cursor.fetch_pandas_all()
    finally:
        cursor.close()

    return df
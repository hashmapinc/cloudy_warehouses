import pandas as pd
from snowflake.connector.pandas_tools import write_pandas
from cloudy_warehouses.snowflake_objects.snowflake_object import SnowflakeObject


# Writer Object
@pd.api.extensions.register_dataframe_accessor("cloudy_warehouses")
class SnowflakeWriter(SnowflakeObject):

    write_success = False
    initialized = False
    sf_credentials = None
    connection = None
    sql_statement = str
    table_columns = []

    def __init__(self, df: pd.DataFrame):
        self.df = df

    # method that creates Snowflake connection and configures Snowflake credentials
    def initialize_snowflake(self,
                             database: str,
                             schema: str,
                             sf_username: str=None,
                             sf_password: str=None,
                             sf_account: str=None):

        if not self.initialized:

            # calls method to configure Snowflake credentials
            self.sf_credentials = self.configure_credentials(
                sf_username=sf_username,
                sf_password=sf_password,
                sf_account=sf_account
            )

            # calls method to connect to Snowflake using the sf_credentials variable
            self.connection = self.get_snowflake_connection(
                user=self.sf_credentials['user'],
                pswd=self.sf_credentials['pass'],
                acct=self.sf_credentials['acct'],
                database=database,
                schema=schema
            )
        self.initialized = True

    # Uploads data from a pandas dataframe to an existing Snowflake table
    def write_snowflake(self,
                        database: str,
                        schema: str,
                        table: str,
                        sf_username: str=None,
                        sf_password: str=None,
                        sf_account: str=None):

        try:
            # initialize Snowflake connection and configure credentials
            self.initialize_snowflake(
                             database=database,
                             schema=schema,
                             sf_username=sf_username,
                             sf_password=sf_password,
                             sf_account=sf_account
                             )

            # calls method to write data in a pandas dataframe to an existing Snowflake table
            success, nchunks, nrows, _ = write_pandas(
                        conn=self.connection,
                        df=self.df,
                        table_name=table
                        )
            self.write_success = success

        finally:
            self.connection.close()

        if self.write_success:
            return "successfully wrote to Snowflake Table"

        else:
            return "not successful"

    # method that creates a Snowflake table and writes pandas dataframe to table
    def create_snowflake(self,
                         database: str,
                         schema: str,
                         table: str,
                         sf_username: str=None,
                         sf_password: str=None,
                         sf_account: str=None):

        try:
            # initialize Snowflake connection and configure credentials
            self.initialize_snowflake(
                             database=database,
                             schema=schema,
                             sf_username=sf_username,
                             sf_password=sf_password,
                             sf_account=sf_account
                             )
            # for loop to generate a string of columns for sql statement
            for key in self.df.keys():
                if key != self.df.keys()[-1]:
                    self.table_columns.append(key + ' variant, ')
                else:
                    self.table_columns.append(key + ' variant')

            # sql statement to be executed in Snowflake
            self.sql_statement = f"CREATE OR REPLACE TABLE {database}.{schema}.{table}({''.join(self.table_columns)})"

            # execute sql statement
            cursor = self.connection.cursor()
            cursor.execute(self.sql_statement)

            # calls method to write data in a pandas dataframe to an existing Snowflake table
            success, nchunks, nrows, _ = write_pandas(
                conn=self.connection,
                df=self.df,
                table_name=table
            )

        finally:
            # close connection and cursor
            self.connection.close()
            cursor.close()

        return "successfully created and wrote to Snowflake Table"
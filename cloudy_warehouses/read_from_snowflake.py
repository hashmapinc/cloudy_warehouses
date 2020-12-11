from cloudy_warehouses.snowflake_objects.snowflake_object import SnowflakeObject
from snowflake import connector


# Reads from Snowflake and returns pandas dataframe
class SnowflakeReader(SnowflakeObject):

    def read(self, database: str, schema: str, table: str, sf_username: str = None, sf_password: str = None, sf_account: str = None):

        # calls function to configure Snowflake credentials
        sf_credentials = self.configure_credentials(
            sf_username=sf_username,
            sf_password=sf_password,
            sf_account=sf_account
            )
        
        # calls function to connect to Snowflake using the sf_credentials variable
        connection = self.get_snowflake_connection(
            user=sf_credentials['user'],
            pswd=sf_credentials['pass'],
            acct=sf_credentials['acct']
            )
        
        # calls function to return data in a Snowflake table as a pandas dataframe
        return self.get_pandas_dataframe(
            connection=connection,
            database=database,
            schema=schema,
            table=table
            )

    def list_tables(self, database: str, sf_username: str = None, sf_password: str = None, sf_account: str = None):
        # calls function to configure Snowflake credentials
        sf_credentials = self.configure_credentials(
            sf_username=sf_username,
            sf_password=sf_password,
            sf_account=sf_account
        )

        # calls function to connect to Snowflake using the sf_credentials variable
        connection = self.get_snowflake_connection(
            user=sf_credentials['user'],
            pswd=sf_credentials['pass'],
            acct=sf_credentials['acct']
        )

        # calls function to return data in a Snowflake table as a pandas dataframe
        return self.get_snowflake_tables(
            connection=connection,
            database=database
            )

    # reads data from a Snowflake table as a pandas dataframe
    def get_pandas_dataframe(self, connection, database, schema, table):
        try:
            cursor = connection.cursor()
            cursor.execute(f'select * from {database}.{schema}.{table}')
            df = cursor.fetch_pandas_all()
        finally:
            cursor.close()

        return df

    # reads data from a Snowflake table as a pandas dataframe
    def get_snowflake_tables(self, connection, database):

        try:
            cursor = connection.cursor()
            cursor.execute(
                f"SELECT TABLE_CATALOG, TABLE_SCHEMA, TABLE_NAME FROM {database}.INFORMATION_SCHEMA.TABLES;")
            tables = cursor.fetch_pandas_all()
        finally:
            cursor.close()

        return tables


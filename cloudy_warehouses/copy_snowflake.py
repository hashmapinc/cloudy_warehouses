from cloudy_warehouses.snowflake_objects.snowflake_object import SnowflakeObject


# Copier Object
class SnowflakeCopier(SnowflakeObject):
    initialized = False
    sf_credentials = None
    connection = None
    sql_statement = str

    # method that creates Snowflake connection and configures Snowflake credentials
    def initialize_snowflake(self,
                             database: str,
                             schema: str,
                             sf_username: str = None,
                             sf_password: str = None,
                             sf_account: str = None):

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

    # method that creates a copy of a Snowflake table
    def clone(self,
             database: str,
             schema: str,
             new_table: str,
             source_table: str,
             sf_username: str = None,
             sf_password: str = None,
             sf_account: str = None):

        try:
            # initialize Snowflake connection and configure credentials
            self.initialize_snowflake(
                database=database,
                schema=schema,
                sf_username=sf_username,
                sf_password=sf_password,
                sf_account=sf_account
            )

            # sql statement to be executed in Snowflake
            self.sql_statement = f"CREATE OR REPLACE TABLE {database}.{schema}.{new_table} CLONE {source_table}"

            # execute sql statement
            cursor = self.connection.cursor()
            cursor.execute(self.sql_statement)

        finally:
            # close connection and cursor
            self.connection.close()
            cursor.close()

        return "successfully created a Snowflake Table copy"

    # method that creates an empty copy of a Snowflake table
    def clone_empty(self,
                    database: str,
                    schema: str,
                    new_table: str,
                    source_table: str,
                    sf_username: str = None,
                    sf_password: str = None,
                    sf_account: str = None):

        try:
            # initialize Snowflake connection and configure credentials
            self.initialize_snowflake(
                database=database,
                schema=schema,
                sf_username=sf_username,
                sf_password=sf_password,
                sf_account=sf_account
            )

            # sql statement to be executed in Snowflake
            self.sql_statement = f"CREATE OR REPLACE TABLE {database}.{schema}.{new_table} LIKE {source_table}"

            # execute sql statement
            cursor = self.connection.cursor()
            cursor.execute(self.sql_statement)

        finally:
            # close connection and cursor
            self.connection.close()
            cursor.close()

        return "successfully created an empty Snowflake Table"
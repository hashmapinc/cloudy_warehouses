import yaml, os
from snowflake import connector


# Reads from Snowflake and returns pandas dataframe
class SnowflakeLister:

    def list_tables(database: str, sf_username: str = None, sf_password: str = None, sf_account: str = None):

        # configures Snowflake credentials
        def configure_credentials(sf_username: str = None, sf_password: str = None, sf_account: str = None):

            # determines where to look for Snowflake credentials
            if not sf_username or not sf_password or not sf_account:
                # Path to Snowflake credentials file
                __profile_path: str = os.path.join(os.getenv("CLOUDY_HOME"),
                                                   '.cloudy_warehouses/configuration_profiles.yml')

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
        def get_snowflake_connection(user, pswd, acct, database=None):
            connection = connector.connect(
                user=user,
                password=pswd,
                account=acct,
                database=database
            )

            return connection

        # reads data from a Snowflake table as a pandas dataframe
        def get_snowflake_tables(connection, database):

            try:
                cursor = connection.cursor()
                cursor.execute(f"SELECT TABLE_CATALOG, TABLE_SCHEMA, TABLE_NAME FROM {database}.INFORMATION_SCHEMA.TABLES;")
                tables = cursor.fetch_pandas_all()
            finally:
                cursor.close()

            return tables

        # calls function to configure Snowflake credentials
        sf_credentials = configure_credentials(
            sf_username=sf_username,
            sf_password=sf_password,
            sf_account=sf_account
        )

        # calls function to connect to Snowflake using the sf_credentials variable
        connection = get_snowflake_connection(
            user=sf_credentials['user'],
            pswd=sf_credentials['pass'],
            acct=sf_credentials['acct']
        )

        # calls function to return data in a Snowflake table as a pandas dataframe
        return get_snowflake_tables(connection=connection,
                                    database=database
                                    )


import yaml
import os
from snowflake import connector


class SnowflakeObject:

    # configures Snowflake credentials
    def configure_credentials(self, sf_username: str = None, sf_password: str = None, sf_account: str = None):

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
    def get_snowflake_connection(self, user, pswd, acct, database=None, schema=None):
        connection = connector.connect(
            user=user,
            password=pswd,
            account=acct,
            database=database,
            schema=schema
        )

        return connection

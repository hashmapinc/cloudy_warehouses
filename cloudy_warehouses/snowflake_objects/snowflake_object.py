import yaml
import os
import logging
from snowflake import connector


class SnowflakeObject:
    """Base class for all Snowflake Object types.
    This object holds the methods needed to connect to Snowflake and configure Snowflake credentials."""

    # True once Snowflake configured and connection established
    initialized = False
    # credentials used to establish Snowflake connection
    sf_credentials = None
    # Snowflake connection object
    connection = False
    # cursor object
    cursor = False
    # logger object
    _logger = logging.getLogger()
    # log message
    log_message = str

    def initialize_snowflake(self, database: str = None, schema: str = None, sf_username: str = None, sf_password: str = None,
                             sf_account: str = None):
        """method that creates Snowflake connection and configures Snowflake credentials"""

        if not self.initialized:
            # calls method to configure Snowflake credentials
            self.configure_credentials(
                sf_username=sf_username,
                sf_password=sf_password,
                sf_account=sf_account
            )
            # calls method to connect to Snowflake using the sf_credentials variable
            self.get_snowflake_connection(
                user=self.sf_credentials['user'],
                pswd=self.sf_credentials['pass'],
                acct=self.sf_credentials['acct'],
                database=database,
                schema=schema
            )

        self.initialized = True

    def configure_credentials(self, sf_username: str = None, sf_password: str = None, sf_account: str = None):
        """configures Snowflake credentials"""

        # determines where to look for Snowflake credentials
        if not sf_username or not sf_password or not sf_account:
            # Path to Snowflake credentials file
            __profile_path: str = os.path.join(os.getenv("CLOUDY_HOME"),
                                               '.cloudy_warehouses/configuration_profiles.yml')
            with open(__profile_path, 'r') as stream:
                self.sf_credentials = yaml.safe_load(stream)['profiles']['snowflake']

            # checks if user has configured credentials file
            if self.sf_credentials['user'] == '<your snowflake username>' or self.sf_credentials['pass'] == \
                    '<your snowflake password>' or self.sf_credentials['acct'] == '<your snowflake account>':

                self.log_message = f"Please configure your credentials at {__profile_path} or pass your credentials " \
                                   f"as arguments when calling this method."
                self._logger.error(self.log_message)
                self.sf_credentials = None
                return False

        else:
            # uses passed in variables as Snowflake credentials
            self.sf_credentials = {
                'user': sf_username,
                'pass': sf_password,
                'acct': sf_account
            }

        return True

    def get_snowflake_connection(self, user, pswd, acct, database=None, schema=None):
        """establishes a connection with snowflake"""
        self.connection = connector.connect(
            user=user,
            password=pswd,
            account=acct,
            database=database,
            schema=schema
        )

        return True

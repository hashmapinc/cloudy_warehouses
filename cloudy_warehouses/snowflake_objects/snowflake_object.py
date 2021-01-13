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

    def initialize_snowflake(self, database: str = None, schema: str = None, username: str = None, password: str = None,
                             account: str = None, role: str = None, warehouse: str = None):
        """method that creates Snowflake connection and configures Snowflake credentials"""

        if not self.initialized:
            # calls method to configure Snowflake credentials
            self.configure_credentials(
                username=username,
                password=password,
                account=account,
                role=role,
                warehouse=warehouse,
                database=database,
                schema=schema
            )
            # calls method to connect to Snowflake using the sf_credentials variable
            self.get_snowflake_connection(
                user=self.sf_credentials['user'],
                pswd=self.sf_credentials['pass'],
                acct=self.sf_credentials['acct'],
                database=self.sf_credentials['database'],
                schema=self.sf_credentials['schema']
            )

        self.initialized = True

    def configure_credentials(self, username: str = None, password: str = None, account: str = None,
                              role: str = None, warehouse: str = None, database: str = None, schema: str = None):
        """configures Snowflake credentials for session"""

        # Path to Snowflake credentials file
        __profile_path: str = os.path.join(os.getenv("CLOUDY_HOME"),
                                           '.cloudy_warehouses/configuration_profiles.yml')
        with open(__profile_path, 'r') as stream:
            self.sf_credentials = yaml.safe_load(stream)['profiles']['snowflake']

        # overwrite default credentials with passed in credentials if applicable
        if username:
            self.sf_credentials['user'] = username
        if password:
            self.sf_credentials['pass'] = password
        if account:
            self.sf_credentials['acct'] = account
        if database:
            self.sf_credentials['database'] = database
        if schema:
            self.sf_credentials['schema'] = schema

        # role is None if none is given or configured as default
        if role or self.sf_credentials['role'] == '<your snowflake role>':
            self.sf_credentials['role'] = role

        # warehouse is None if none is given or configured as default
        if warehouse or self.sf_credentials['warehouse'] == '<your snowflake warehouse>':
            self.sf_credentials['warehouse'] = warehouse

        # checks if user has configured or passed credentials
        if self.sf_credentials['user'] == '<your snowflake username>' or self.sf_credentials['pass'] == \
                '<your snowflake password>' or self.sf_credentials['acct'] == '<your snowflake account>' or \
                self.sf_credentials['database'] == '<your snowflake database>':

            self.log_message = f"Please configure your credentials at {__profile_path} or pass your credentials " \
                               f"as arguments when calling this method."
            self._logger.error(self.log_message)
            self.sf_credentials = None
            return False

        return True

    def get_snowflake_connection(self, user, pswd, acct, database=None, schema=None, role=None):
        """establishes a connection with snowflake"""
        self.connection = connector.connect(
            user=user,
            password=pswd,
            account=acct,
            role=role,
            database=database,
            schema=schema,

        )

        return True

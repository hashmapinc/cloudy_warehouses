import pandas as pd
from snowflake.sqlalchemy import URL
from sqlalchemy import create_engine
from snowflake.connector.pandas_tools import pd_writer
from cloudy_warehouses.snowflake_objects.snowflake_object import SnowflakeObject


@pd.api.extensions.register_dataframe_accessor("cloudy_warehouses")
class SnowflakeWriter(SnowflakeObject):
    """Writer Object: contains write_snowflake and create_snowflake methods."""

    # True when write_snowflake successfully runs
    write_success = False
    # SQL Alchemy Engine
    engine = None

    def __init__(self, df: pd.DataFrame):
        self.df = df

    def write_snowflake(self, table: str, database: str = None, schema: str = None, sf_username: str = None,
                        sf_password: str = None, sf_account: str = None, sf_role: str = None, sf_warehouse: str = None):
        """Uploads data from a pandas dataframe to an existing Snowflake table."""

        try:
            # initialize Snowflake connection and configure credentials
            self.initialize_snowflake(
                             database=database,
                             schema=schema,
                             sf_username=sf_username,
                             sf_password=sf_password,
                             sf_account=sf_account,
                             sf_warehouse=sf_warehouse,
                             sf_role=sf_role
                             )

            # create SQL Alchemy engine
            self.engine = create_engine(URL(
                user=self.sf_credentials['user'],
                password=self.sf_credentials['pass'],
                account=self.sf_credentials['acct'],
                database=self.sf_credentials['database'],
                schema=self.sf_credentials['schema']
            ))

            self.connection = self.engine.connect()

            # use warehouse if not None
            if self.sf_credentials['warehouse']:
                self.connection.execute(f"use warehouse {self.sf_credentials['warehouse']}")

            # use role if not None
            if self.sf_credentials['role']:
                self.connection.execute(f"use role {self.sf_credentials['role']}")

            # calls method to write data in a pandas dataframe to an existing Snowflake table
            # will create a new snowflake table if the given table name does not exist
            self.df.to_sql(name=table, con=self.engine, index=False, if_exists='append', method=pd_writer)

            self.write_success = True

        # catch and log error
        except Exception as e:
            self.log_message = e
            self._logger.error(self.log_message)
            return False

        finally:
            # close connection
            if self.connection:
                self.connection.close()
            # close engine
            if self.engine:
                self.engine.dispose()

        if self.write_success:
            self.log_message = f"Successfully wrote to the {table} Snowflake table"
            self._logger.info(self.log_message)
            return self.write_success
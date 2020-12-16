from cloudy_warehouses.snowflake_objects.snowflake_object import SnowflakeObject


class SnowflakeReader(SnowflakeObject):
    """Contains read and list_tables methods."""

    def read(self, database: str, schema: str, table: str, sf_username: str = None, sf_password: str = None, sf_account: str = None):
        """reads a table from Snowflake and returns a pandas dataframe of that table."""

        try:
            # configure and connect to Snowflake
            self.initialize_snowflake(
                database=database,
                schema=schema,
                sf_username=sf_username,
                sf_password=sf_password,
                sf_account=sf_account
            )

            # calls function to return data in a Snowflake table as a pandas dataframe
            df = self.get_pandas_dataframe(
                connection=self.connection,
                database=database,
                schema=schema,
                table=table
            )

        # catch and log error
        except Exception as e:
            self.log_message = e
            self._logger.error(self.log_message)
            return False

        finally:
            # close connection and cursor
            if self.connection:
                self.connection.close()
            if self.cursor:
                self.cursor.close()

        # log successful clone
        self.log_message = f"Successfully read from {database}.{schema}.{table}"
        self._logger.info(self.log_message)
        return df

    def list_tables(self, database: str, sf_username: str = None, sf_password: str = None, sf_account: str = None):
        """lists all tables in the specified Snowflake database. The list is returned as a pandas dataframe."""

        try:
            # configure and connect to Snowflake
            self.initialize_snowflake(
                database=database,
                sf_username=sf_username,
                sf_password=sf_password,
                sf_account=sf_account
            )

            # calls function to return data in a Snowflake table as a pandas dataframe
            df = self.get_snowflake_tables(
                connection=self.connection,
                database=database
                )

        # catch and log error
        except Exception as e:
            self.log_message = e
            self._logger.error(self.log_message)
            return False

        finally:
            # close connection and cursor
            if self.connection:
                self.connection.close()
            if self.cursor:
                self.cursor.close()

        # log successful clone
        self.log_message = f"Successfully listed tables from {database}"
        self._logger.info(self.log_message)
        return df

    def get_pandas_dataframe(self, connection, database, schema, table):
        """Reads data from a Snowflake table as a pandas dataframe."""
        self.cursor = connection.cursor()
        self.cursor.execute(f'select * from {database}.{schema}.{table}')
        df = self.cursor.fetch_pandas_all()

        return df

    def get_snowflake_tables(self, connection, database):
        """Reads data from a Snowflake table as a pandas dataframe."""
        self.cursor = connection.cursor()
        self.cursor.execute(
            f"SELECT TABLE_CATALOG, TABLE_SCHEMA, TABLE_NAME FROM {database}.INFORMATION_SCHEMA.TABLES;")
        tables = self.cursor.fetch_pandas_all()

        return tables


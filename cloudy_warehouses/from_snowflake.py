from cloudy_warehouses.snowflake_objects.snowflake_object import SnowflakeObject


class SnowflakeReader(SnowflakeObject):
    """Contains read and list_tables methods."""

    def read(self, table: str, username: str = None, password: str = None, account: str = None,
             database: str = None, schema: str = None, role: str = None, warehouse: str = None):
        """reads a table from Snowflake and returns a pandas dataframe of that table."""

        try:
            # configure and connect to Snowflake
            self.initialize_snowflake(
                database=database,
                schema=schema,
                username=username,
                password=password,
                account=account,
                warehouse=warehouse,
                role=role
            )

            # calls function to return data in a Snowflake table as a pandas dataframe
            df = self.get_pandas_dataframe(
                connection=self.connection,
                database=self.sf_credentials['database'],
                schema=self.sf_credentials['schema'],
                table=table,
                warehouse=self.sf_credentials['warehouse']
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
        self.log_message = f"Successfully read from {self.sf_credentials['database']}.{self.sf_credentials['database']}.{table}"
        self._logger.info(self.log_message)
        return df

    def list_tables(self, database: str = None, username: str = None, password: str = None, account: str = None,
                    role: str = None, warehouse: str = None):
        """lists all tables in the specified Snowflake database. The list is returned as a pandas dataframe."""

        try:
            # configure and connect to Snowflake
            self.initialize_snowflake(
                database=database,
                username=username,
                password=password,
                account=account,
                role=role,
                warehouse=warehouse
            )

            # calls function to return data in a Snowflake table as a pandas dataframe
            df = self.get_snowflake_tables(
                connection=self.connection,
                database=self.sf_credentials['database'],
                warehouse=self.sf_credentials['warehouse']
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
        self.log_message = f"Successfully listed tables from {self.sf_credentials['database']}"
        self._logger.info(self.log_message)
        return df

    def get_pandas_dataframe(self, connection, database, schema, table, warehouse: str = None):
        """Reads data from a Snowflake table as a pandas dataframe."""
        self.cursor = connection.cursor()

        # use warehouse if not None
        if warehouse:
            self.cursor.execute(f"use warehouse {warehouse};")

        self.cursor.execute(f'select * from {database}.{schema}.{table}')
        df = self.cursor.fetch_pandas_all()

        return df

    def get_snowflake_tables(self, connection, database, warehouse: str = None):
        """Reads data from a Snowflake table as a pandas dataframe."""
        self.cursor = connection.cursor()

        # use warehouse if not None
        if warehouse:
            self.cursor.execute(f"use warehouse {warehouse};")

        self.cursor.execute(
            f"SELECT TABLE_CATALOG, TABLE_SCHEMA, TABLE_NAME FROM {database}.INFORMATION_SCHEMA.TABLES;")
        tables = self.cursor.fetch_pandas_all()

        return tables


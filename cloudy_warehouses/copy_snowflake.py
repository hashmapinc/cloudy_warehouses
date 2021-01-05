from cloudy_warehouses.snowflake_objects.snowflake_object import SnowflakeObject


# Copier Object
class SnowflakeCopier(SnowflakeObject):
    """Class that holds the clone and clone_empty methods."""

    # sql that is run by the cursor object
    sql_statement = str

    def clone(self, new_table: str, source_table: str, source_schema: str = None, source_database: str = None,
              database: str = None, schema: str = None, username: str = None, password: str = None,
              account: str = None, role: str = None, warehouse: str = None):
        """method that creates a copy of a Snowflake table."""
        try:
            # initialize Snowflake connection and configure credentials
            self.initialize_snowflake(
                database=database,
                schema=schema,
                username=username,
                password=password,
                account=account,
                warehouse=warehouse,
                role=role
            )

            # build sql statement to be executed by the cursor object
            if source_database and source_schema:
                self.sql_statement = f"CREATE OR REPLACE TABLE {self.sf_credentials['database']}.{self.sf_credentials['schema']}.{new_table} CLONE " \
                                     f"{source_database}.{source_schema}.{source_table}"

            elif source_schema and not source_database:
                self.sql_statement = f"CREATE OR REPLACE TABLE {self.sf_credentials['database']}.{self.sf_credentials['schema']}.{new_table} CLONE " \
                                     f"{source_schema}.{source_table}"

            elif not source_schema and not source_database:
                self.sql_statement = f"CREATE OR REPLACE TABLE {self.sf_credentials['database']}.{self.sf_credentials['schema']}.{new_table} CLONE " \
                                     f"{source_table}"

            else:
                self.log_message = "Error: please call this method with the proper values. Example: If you call this " \
                                   "method with the 'source_database' parameter, " \
                                   "you must include a 'source_schema' parameter as well"
                self._logger.error(self.log_message)
                return False

            # execute sql statement
            self.cursor = self.connection.cursor()

            # use warehouse if not None
            if self.sf_credentials['warehouse']:
                self.cursor.execute(f"use warehouse {self.sf_credentials['warehouse']};")

            self.cursor.execute(self.sql_statement)

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
        self.log_message = f"Successfully cloned {source_table} into {self.sf_credentials['database']}.{self.sf_credentials['schema']}.{new_table}"
        self._logger.info(self.log_message)
        return True

    def clone_empty(self, new_table: str, source_table: str, database: str = None, schema: str = None,
                    source_database: str = None, source_schema: str = None, username: str = None,
                    password: str = None, account: str = None, role: str = None, warehouse: str = None):
        """method that creates an empty copy of a Snowflake table."""

        try:
            # initialize Snowflake connection and configure credentials
            self.initialize_snowflake(
                database=database,
                schema=schema,
                username=username,
                password=password,
                account=account,
                role=role,
                warehouse=warehouse
            )

            # build sql statement to be executed by the cursor object
            if source_database and source_schema:
                self.sql_statement = f"CREATE OR REPLACE TABLE {self.sf_credentials['database']}.{self.sf_credentials['schema']}.{new_table} LIKE " \
                                     f"{source_database}.{source_schema}.{source_table}"

            elif source_schema and not source_database:
                self.sql_statement = f"CREATE OR REPLACE TABLE {self.sf_credentials['database']}.{self.sf_credentials['schema']}.{new_table} LIKE " \
                                     f"{source_schema}.{source_table}"

            elif not source_schema and not source_database:
                self.sql_statement = f"CREATE OR REPLACE TABLE {self.sf_credentials['database']}.{self.sf_credentials['schema']}.{new_table} LIKE " \
                                     f"{source_table}"

            else:
                self.log_message = "Error: please call this method with viable values. Example: If you call this " \
                                   "method with the 'source_database' parameter, " \
                                   "you must include a 'source_schema' parameter as well"
                self._logger.error(self.log_message)
                return False

            # execute sql statement
            self.cursor = self.connection.cursor()

            # use warehouse if not None
            if self.sf_credentials['warehouse']:
                self.cursor.execute(f"use warehouse {self.sf_credentials['warehouse']};")

            self.cursor.execute(self.sql_statement)

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
        self.log_message = f"Successfully cloned an empty version of {source_table} into " \
                           f"{self.sf_credentials['database']}.{self.sf_credentials['schema']}.{new_table}"
        self._logger.info(self.log_message)
        return True

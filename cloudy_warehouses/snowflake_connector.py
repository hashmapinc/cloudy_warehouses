from snowflake import connector


# establishes a connection with snowflake
def get_snowflake_connection(user, pswd, acct, database=None, schema=None):
    connection = connector.connect(
        user = user,
        password = pswd,
        account = acct,
        database = database,
        schema = schema
    )

    return connection


    
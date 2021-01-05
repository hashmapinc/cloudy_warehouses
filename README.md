<!---
# Modifications © 2020 Hashmap, Inc
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
-->
# cloudy_warehouses

Extension to pandas to allow for simplified interactions with cloud-based data-platforms.

## Snowflake Support
The cloudy_warehouses pandas extension currently supports: 
- reading from a Snowflake table
- writing to an existing Snowflake table from a pandas dataframe
- creating a new Snowflake table from a pandas dataframe
- creating a clone of an existing Snowflake table
- creating an empty clone of an existing Snowflake table (clones columns, not column data)
- listing Snowflake tables in a database
## How To Use cloudy_warehouses

**Installation**

`pip install cloudy-warehouses`

**Configuration** 

Upon installation, create an empty `.py` file. Then, configure the python file in the following way:
```python
import pandas as pd 
import cloudy_warehouses

```
Run this empty file. After you run the file, a configuration file will be created in your HOME directory.
The path to the configuration file is: `$HOME/.cloudy_warehouses/configuration_profiles.yml`

For Windows user: use $USERPROFILE instead of $HOME variable

The configuration file is a YAML file with the following format
```yaml
profiles:
  snowflake:
    user: <your snowflake username>
    pass: <your snowflake password>
    acct: <your snowflake account>
    role: <your snowflake role>
    warehouse: <your snowflake warehouse>
    database: <your snowflake database>
    schema: <your snowflake schema>
```
The user, pass, acct, database, schema, values all should be filled in with your desired Snowflake credentials and connection details. The variables in this
file serve as default arguments when calling a cloudy_warehouses method. Role and warehouse can be filled in as well, but they are optional arguments when connecting to Snowflake.



## API
The intent has been to keep the API as simple as possible by minimally extending the pandas API and supporting, for the most part, the same functionality.

### Optional Arguments
There are two methods for passing optional arguments into a method.
1. The configuration file
2. Directly pass in the arguments when calling the method

The variables saved in the configuration file serve as default arguments for the methods to use.
However, you tell the method to use different credentials by passing in arguments directly. The method will use the passed in arguments 
instead of the default arguments saved in `configuration_profiles.yml`. 

For example, if I had the `database` variable saved in the `configuration_profiles.yml` as `database_1`, but passed in `database = database_2` directly into the method,
the method would use `database_2` instead of `database_1`. 

However, if I choose to not directly pass a `database` argument in, the method will use the 
`database_1` because it is the default. The passed in arguments take priority over the default variables saved in `configuration_profiles.yml`.
### Reading a from a Snowflake table
```python
pandas.read_snowflake(table: str, 
                      username: str = None, 
                      password: str = None, 
                      account: str = None,
                      database: str = None, 
                      schema: str = None, 
                      role: str = None, 
                      warehouse: str = None
                     )
```
This method reads from a Snowflake table and returns the data in said tables as a pandas DataFrame.

### Listing all tables in a Snowflake database
```python
pandas.list_snowflake_tables(database: str = None, 
                             username: str = None, 
                             password: str = None, 
                             account: str = None,
                             role: str = None, 
                             warehouse: str = None
                            )
```
This method returns all of the tables in a Snowflake database as a pandas DataFrame. 

### Writing to a Snowflake table
```python
pandas.DataFrame.cloudy_warehouses.write_snowflake(table: str, 
                                                   database: str = None, 
                                                   schema: str = None, 
                                                   username: str = None,
                                                   password: str = None, 
                                                   account: str = None, 
                                                   role: str = None, 
                                                   warehouse: str = None
                                                  )
```
This method writes to a Snowflake table and informs you on success. This method works when writing to either an existing Snowflake table or a previously non-existing Snowflake table. 
If the table that you provide does not exist, this method creates a new Snowflake table and writes to it. 

### Creating a clone of an existing Snowflake table
```python
pandas.clone_snowflake(new_table: str, 
                       source_table: str, 
                       source_schema: str = None, 
                       source_database: str = None,
                       database: str = None, 
                       schema: str = None, 
                       username: str = None, 
                       password: str = None,
                       account: str = None, 
                       role: str = None, 
                       warehouse: str = None
                      )           
```
This method creates a clone of an existing Snowflake table. The `new_table` variable is the new table that will
be created after the method is called. The `source_table` variable is the existing Snowflake table that is being cloned.
The optional `source_database` and `source_schema` variables are the database and schema in which the `source_table` resides.
If you plan to clone an existing table from the schema and database that the `new_table` will reside in, you do not need to 
include `source_database` and `source_schema` variables.

### Creating an empty clone of an existing Snowflake table
```python
pandas.clone_empty_snowflake(new_table: str, 
                             source_table: str,
                             source_database: str = None, 
                             source_schema: str = None,
                             database: str = None, 
                             schema: str = None,
                             username: str = None,
                             password: str = None, 
                             account: str = None, 
                             role: str = None, 
                             warehouse: str = None
                            )           
```
This method creates an empty clone of an existing Snowflake table. This means that of the columns from the `source_table`
are copied into the `new_table`, but the `new_table` does not have any data within its columns. The `new_table` variable is the new table that will
be created after the method is called. The `source_table` variable is the existing Snowflake table that is being cloned. 
The optional `source_database` and `source_schema` variables are the database and schema in which the `source_table` resides.
If you plan to clone an existing table from the schema and database that the `new_table` will reside in, you do not need to 
include `source_database` and `source_schema` variables.

## Examples

__Reading, Writing, and Listing (using configuration file)__
```python
import pandas as pd 
import cloudy_warehouses

pd.list_snowflake_tables(role='SNOWFLAKE_ROLE')

df_to_write = pd.DataFrame.from_dict({'a': [1, 2, 3], 'b': [2, 3, 5]})

df_to_write.cloudy_warehouses.write_snowflake(table='SNOWFLAKE_TABLE', role='SNOWFLAKE_ROLE')

df_read_from_snowflake = pd.read_snowflake(table='SNOWFLAKE_TABLE', role='SNOWFLAKE_ROLE')
```
The methods called in this example use the `database`, `schema`, `warehouse`, and account credentials stored in `configuration_profiles.yml`.
However, the `role` is directly passed in and therefore overwrites the default `role` saved in the configuraiton file.

__Cloning and Empty Cloning (using configuration file)__
```python
import pandas as pd 
import cloudy_warehouses

df = pd.DataFrame.from_dict({'COL_1': ['hello', 'there'], 'COL_2': [10, 20], 'COL_3': [10, 20]})

pd.clone_snowflake(
    new_table='SNOWFLAKE_TABLE',
    source_table='SNOWFLAKE_TABLE_TO_BE_CLONED',
    source_schema='SNOWFLAKE_SCHEMA_THAT_HOLDS_THE_SOURCE_TABLE'
    )

pd.clone_empty_snowflake(
    new_table='SNOWFLAKE_TABLE',
    source_table='SNOWFLAKE_TABLE_TO_BE_CLONED',
    source_schema='SNOWFLAKE_SCHEMA_THAT_HOLDS_THE_SOURCE_TABLE',
    source_database='SNOWFLAKE_DATABASE_THAT_HOLDS_THE_SOURCE_SCHEMA'
    )
```
The methods called in this example use the `database`, `schema`, `warehouse`, `role` and account credentials stored in `configuration_profiles.yml`.
The newly cloned table will be saved in the default `database` and `schema` in the configuration file.

__Reading, Writing, and Listing (using optional Snowflake credentials arguments)__

```python
import pandas as pd
import cloudy_warehouses

pd.list_snowflake_tables(
    database='SNOWFLAKE_DATABASE',
    username='my_snowflake_username',
    password='my_snowflake_password',
    account='my_snowflake_account',
    warehouse='my_snowflake_warehouse',
    role='my_snowflake_role'
)

df_to_write = pd.DataFrame.from_dict({'a': [1, 2, 3], 'b': [2, 3, 5]})

df_to_write.cloudy_warehouses.write_snowflake(
    database='SNOWFLAKE_DATABASE',
    schema='SNOWFLAKE_SCHEMA',
    table='SNOWFLAKE_TABLE',
    username='my_snowflake_username',
    password='my_snowflake_password',
    account='my_snowflake_account'
)

df_read_from_snowflake = pd.read_snowflake(
    schema='SNOWFLAKE_SCHEMA',
    table='SNOWFLAKE_TABLE',
    username='my_snowflake_username',
    password='my_snowflake_password',
    account='my_snowflake_account',
    role='my_snowflake_role'
)
```
The arguments passed in here will be used instead of the default variables saved in the configuration file.
For example, in last method called, the schema, username, password, account, and role are all passed in. However, this 
method will use the default `database` variable because there is not one directly passed in.

__Cloning and Empty Cloning (using optional Snowflake credentials arguments)__
```python
import pandas as pd 
import cloudy_warehouses

df = pd.DataFrame.from_dict({'COL_1': ['hello', 'there'], 'COL_2': [10, 20], 'COL_3': [10, 20]})

pd.clone_snowflake(
    database='SNOWFLAKE_DATABASE', 
    schema='SNOWFLAKE_SCHEMA',
    new_table='SNOWFLAKE_TABLE',
    source_table='SNOWFLAKE_TABLE_TO_BE_CLONED',
    source_schema='SNOWFLAKE_SCHEMA_THAT_HOLDS_THE_SOURCE_TABLE',
    source_database='SNOWFLAKE_DATABASE_THAT_HOLDS_THE_SOURCE_SCHEMA',
    username='my_snowflake_username', 
    password='my_snowflake_password', 
    account='my_snowflake_account',
    warehouse='my_snowflake_warehouse',
    role='my_snowflake_role'
    )

pd.clone_empty_snowflake(
    database='SNOWFLAKE_DATABASE', 
    schema='SNOWFLAKE_SCHEMA',
    new_table='SNOWFLAKE_TABLE',
    source_table='SNOWFLAKE_TABLE_TO_BE_CLONED',
    username='my_snowflake_username', 
    password='my_snowflake_password', 
    account='my_snowflake_account'
    )
```
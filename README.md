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

`pip install cloudy_warehouses`

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
```
The user, pass, and acct values all need to be filled in with your Snowflake credentials.



## API
The intent has been to keep the API as simple as possible by minimally extending the pandas API and supporting, for the most part, the same functionality.

### Reading a from a Snowflake table
```python
pandas.read_snowflake(database: str, 
                      schema: str, 
                      table: str, 
                      sf_username: str = None, 
                      sf_password: str = None, 
                      sf_account: str = None
                      )
```
This method reads from a Snowflake table and returns the data in said tables as a pandas DataFrame. This method uses your Snowflake credentials stored in the configuration file.
However, you can pass optional arguments (sf_username, sf_password, sf_account) to connect to Snowflake as well.

### Listing all tables in a Snowflake database
```python
pandas.list_snowflake_tables(database: str, 
                             sf_username: str = None, 
                             sf_password: str = None, 
                             sf_account: str = None
                             )
```
This method returns all of the tables in a Snowflake database as a pandas DataFrame. 
There is an option to pass arguments (sf_username, sf_password, sf_account) to connect to Snowflake.

### Writing to a Snowflake table
```python
pandas.DataFrame.cloudy_warehouses.write_snowflake(database: str,
                                                   schema: str, 
                                                   table: str, 
                                                   sf_username: str=None, 
                                                   sf_password: str=None, 
                                                   sf_account: str=None
                                                   )
```
This method writes to a Snowflake table and informs you on success. This method works when writing to either an existing Snowflake table or a previously non-existing Snowflake table. 
If the table that you provide does not exist, this method creates a new Snowflake table and writes to it. 
Like the read_snowflake method, write_snowflake uses your Snowflake credentials stored in the configuration file.
However, you can pass optional arguments (sf_username, sf_password, sf_account) to connect to Snowflake as well.

### Creating a clone of an existing Snowflake table
```python
pandas.clone_snowflake(database: str,
                       schema: str,
                       new_table: str,
                       source_table: str,
                       source_schema: str = None,
                       source_database: str = None,
                       sf_username: str = None,
                       sf_password: str = None,
                       sf_account: str = None
                       )           
```
This method creates a clone of an existing Snowflake table. The `new_table` variable is the new table that will
be created after the method is called. The `source_table` variable is the existing Snowflake table that is being cloned.
The optional `source_database` and `source_schema` variables are the database and schema in which the `source_table` resides.
If you plan to clone an existing table from the schema and database that the `new_table` will reside in, you do not need to 
include `source_database` and `source_schema` variables.
You can use the configuration file or pass optional arguments (sf_username, sf_password, sf_account) to connect to Snowflake.

### Creating an empty clone of an existing Snowflake table
```python
pandas.clone_empty_snowflake(database: str,
                             schema: str,
                             new_table: str,
                             source_table: str,
                             source_schema: str = None,
                             source_database: str = None,
                             sf_username: str = None,
                             sf_password: str = None,
                             sf_account: str = None
                             )           
```
This method creates an empty clone of an existing Snowflake table. This means that of the columns from the `source_table`
are copied into the `new_table`, but the `new_table` does not have any data within its columns. The `new_table` variable is the new table that will
be created after the method is called. The `source_table` variable is the existing Snowflake table that is being cloned. 
The optional `source_database` and `source_schema` variables are the database and schema in which the `source_table` resides.
If you plan to clone an existing table from the schema and database that the `new_table` will reside in, you do not need to 
include `source_database` and `source_schema` variables.
You can use the configuration file or pass optional arguments (sf_username, sf_password, sf_account) to connect to Snowflake.

## Examples

__Reading, Writing, and Listing (using configuration file)__
```python
import pandas as pd 
import cloudy_warehouses

pd.list_snowflake_tables(database='SNOWFLAKE_DATABASE')

df_to_write = pd.DataFrame.from_dict({'a': [1, 2, 3], 'b': [2, 3, 5]})

df_to_write.cloudy_warehouses.write_snowflake(
    database='SNOWFLAKE_DATABASE', 
    schema='SNOWFLAKE_SCHEMA', 
    table='SNOWFLAKE_TABLE'
    )

df_read_from_snowflake = pd.read_snowflake(
    database='SNOWFLAKE_DATABASE', 
    schema='SNOWFLAKE_SCHEMA', 
    table='SNOWFLAKE_TABLE'
    )
```

__Cloning and Empty Cloning (using configuration file)__
```python
import pandas as pd 
import cloudy_warehouses

df = pd.DataFrame.from_dict({'COL_1': ['hello', 'there'], 'COL_2': [10, 20], 'COL_3': [10, 20]})

pd.clone_snowflake(
    database='SNOWFLAKE_DATABASE', 
    schema='SNOWFLAKE_SCHEMA',
    new_table='SNOWFLAKE_TABLE',
    source_table='SNOWFLAKE_TABLE_TO_BE_CLONED',
    source_schema='SNOWFLAKE_SCHEMA_THAT_HOLDS_THE_SOURCE_TABLE'
    )

pd.clone_empty_snowflake(
    database='SNOWFLAKE_DATABASE', 
    schema='SNOWFLAKE_SCHEMA',
    new_table='SNOWFLAKE_TABLE',
    source_table='SNOWFLAKE_TABLE_TO_BE_CLONED',
    source_schema='SNOWFLAKE_SCHEMA_THAT_HOLDS_THE_SOURCE_TABLE',
    source_database='SNOWFLAKE_DATABASE_THAT_HOLDS_THE_SOURCE_SCHEMA'
    )
```

__Reading, Writing, and Listing (using optional Snowflake credentials arguments)__
```python
import pandas as pd
import cloudy_warehouses

pd.list_snowflake_tables(
    database='SNOWFLAKE_DATABASE', 
    sf_username='my_snowflake_username', 
    sf_password='my_snowflake_password', 
    sf_account='my_snowflake_account'
    )

df_to_write = pd.DataFrame.from_dict({'a': [1, 2, 3], 'b': [2, 3, 5]})

df_to_write.cloudy_warehouses.write_snowflake(
    database='SNOWFLAKE_DATABASE', 
    schema='SNOWFLAKE_SCHEMA', 
    table='SNOWFLAKE_TABLE', 
    sf_username='my_snowflake_username', 
    sf_password='my_snowflake_password', 
    sf_account='my_snowflake_account'
    )

df_read_from_snowflake = pd.read_snowflake(
    database='SNOWFLAKE_DATABASE', 
    schema='SNOWFLAKE_SCHEMA', 
    table='SNOWFLAKE_TABLE', 
    sf_username='my_snowflake_username', 
    sf_password='my_snowflake_password', 
    sf_account='my_snowflake_account'
    )
```

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
    sf_username='my_snowflake_username', 
    sf_password='my_snowflake_password', 
    sf_account='my_snowflake_account'
    )

pd.clone_empty_snowflake(
    database='SNOWFLAKE_DATABASE', 
    schema='SNOWFLAKE_SCHEMA',
    new_table='SNOWFLAKE_TABLE',
    source_table='SNOWFLAKE_TABLE_TO_BE_CLONED',
    sf_username='my_snowflake_username', 
    sf_password='my_snowflake_password', 
    sf_account='my_snowflake_account'
    )
```
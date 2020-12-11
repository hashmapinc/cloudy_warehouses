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

Extension to pandas to allow for simple interactions with cloud-based data-platforms.

##Snowflake Support
The cloudy_warehouses pandas extension currently supports reading from, writing to, and listing Snowflake tables in a database.
##How To Use cloudy_warehouses

**Installation**

`pip install cloudy_warehouses`

**Configuration**

Upon installation, a path to a YAML file will be created in your HOME directory. 
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
The user, pass, and acct values all need to be filled in with the appropriate Snowflake credentials.



##API
The intent has been to keep the API as simple as possible by minimally extending the pandas API and supporting, for the most part, the same functionality.

### Reading a from a Snowflake table
__pandas.read_snowflake(database: str, schema: str, table: str, sf_username: str = None, sf_password: str = None, sf_account: str = None)__
This method reads from a Snowflake table and returns the data in said tables as a pandas DataFrame. This method uses your Snowflake credentials stored in the configuration file.
However, you can pass optional arguments (sf_username, sf_password, sf_account) to connect to Snowflake as well.

### Writing to an existing Snowflake table
__pandas.write_snowflake(database: str, schema: str, table: str, sf_username: str=None, sf_password: str=None, sf_account: str=None)__
This method writes to an existing Snowflake table and informs you on success. Like the read_snowflake method, write_snowflake uses your Snowflake credentials stored in the configuration file.
However, you can pass optional arguments (sf_username, sf_password, sf_account) to connect to Snowflake as well.

### Listing all tables in a Snowflake database
__pandas.list_snowflake_tables(database: str, sf_username: str = None, sf_password: str = None, sf_account: str = None)__
This method returns all of the tables in a Snowflake database as a pandas DataFrame. 
There is an option to pass arguments (sf_username, sf_password, sf_account) to connect to Snowflake.

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
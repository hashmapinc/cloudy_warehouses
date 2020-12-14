import platform
import os
import yaml
import pandas as pd
from cloudy_warehouses.from_snowflake import SnowflakeReader
from cloudy_warehouses.to_snowflake import SnowflakeWriter
from cloudy_warehouses.copy_snowflake import SnowflakeCopier

if not os.getenv('CLOUDY_HOME'):
    if platform.system().lower() != 'windows':
        os.environ['CLOUDY_HOME'] = os.getenv('HOME')
    else:
        os.environ['CLOUDY_HOME'] = os.getenv('USERPROFILE')

# Create configuration
profiles_path = os.path.join(os.getenv("CLOUDY_HOME"), ".cloudy_warehouses/configuration_profiles.yml")
default_profiles_path: str = os.path.join(os.path.dirname(__file__), 'configurations/default_configuration_profiles.yml')

#  If the configuration path does not exist - then a default configuration will be created
if not os.path.exists(profiles_path):

    # Set the path for the default configuration if it does not exist
    cloudy_profiles = os.path.join(os.getenv("CLOUDY_HOME"), ".cloudy_warehouses")
    if not os.path.exists(cloudy_profiles):
        os.mkdir(cloudy_profiles)

    # Load the default configuration
    with open(default_profiles_path, 'r') as default_stream:
        profiles_configuration = yaml.safe_load(default_stream)

    # Write the default configuration
    with open(profiles_path, 'w') as stream:
        _ = yaml.dump(profiles_configuration, stream)


# Add ability to read from pandas
pd.read_snowflake = SnowflakeReader().read

# Add ability to list from pandas
pd.list_snowflake_tables = SnowflakeReader().list_tables

# Add ability to copy from pandas
pd.clone_snowflake = SnowflakeCopier().clone

# Add ability to copy empty from pandas
pd.clone_empty_snowflake = SnowflakeCopier().clone_empty

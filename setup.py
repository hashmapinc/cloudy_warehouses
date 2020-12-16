import setuptools

with open("README.md", "r") as fh:
    long_description = fh.read()

setuptools.setup(
    name="cloudy_warehouses",
    version="0.0.3.9",
    author="Hashmap, Inc",
    author_email="accelerators@hashmapinc.com",
    description="Cloudy Warehouses allows for stream-lined interaction between pandas and cloud data platform providers",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/hashmapinc/sales-and-marketing/pocs/sam-kohlleffel-poc/pandas-cloudy-extension",
    packages=setuptools.find_packages(),
    package_data={
        "cloudy_warehouses": ["configurations/default_configuration_profiles.yml"],
    },
    install_requires=[
        'pyarrow==0.17.1',
        'pandas==1.1.4',
        'pyyaml==5.3.1',
        'snowflake-connector-python==2.3.6',
        'snowflake-sqlalchemy==1.2.4',
        'SQLAlchemy==1.3.20'

    ],
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: Apache Software License",
        "Operating System :: OS Independent",
        "Development Status :: 3 - Alpha"
    ],
    python_requires='>=3.7',
)

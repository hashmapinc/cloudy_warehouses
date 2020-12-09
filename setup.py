import setuptools

setuptools.setup(
    name="cloudy_warehouses",
    version="0.0.2.7",
    author="Hashmap, Inc",
    author_email="accelerators@hashmapinc.com",
    description="DO NOT USE - This is a sample program",
    url="https://github.com/hashmapinc/sales-and-marketing/pocs/sam-kohlleffel-poc/pandas-cloudy-extension",
    packages=setuptools.find_packages(),
    package_data={
        "cloudy_warehouses.configurations": ["default_configuration_profiles.yml"],
    },
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: Apache Software License",
        "Operating System :: OS Independent"
    ],
    python_requires='>=3.7',
)

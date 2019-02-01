
import setuptools
with open("README.md", "r") as fh:
    long_description = fh.read()
setuptools.setup(
    name='ibm_wca_plugin',
    version='0.1',
    scripts=['ibm_wca_plugin'],
    author="James Simonson",
    author_email="james.simo@gmail.com",
    description="Airflow Plugin for interacting with IBM Watson Campaign Automation XML API and FTP",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/jamesnsimo/ibm_wca_plugin",
    packages=setuptools.find_packages(),
    classifiers=[
        "Programming Language :: Python :: 3",
         "License :: OSI Approved :: MIT License",
         "Operating System :: OS Independent",
    ],
    entry_points={
        'airflow.plugins': [
            'ibm_wca_plugin = ibm_wca_plugin.ibm_wca_plugin:IbmWcaPlugin'
        ]
    },
)

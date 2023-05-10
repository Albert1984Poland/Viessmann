from setuptools import setup, find_packages

setup(
    name="viessmann_lib_pkg",
    version="1.0.0",
    description="Library for Databricks",
    packages=find_packages(include=["viessmann_lib"]),
    long_description="Long description",
    long_description_content_type="text/markdown",
    setup_requires=['wheel'],
    python_requires=">=3.9"
)


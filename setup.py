#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""The setup script."""

# # from setuptools import setup, find_packages
from setuptools import setup

with open('README.md') as readme_file:
    readme = readme_file.read()

# with open('HISTORY.rst') as history_file:
#     history = history_file.read()

with open('requirements.txt') as req_file:
    requirements = req_file.read()

# setup_requirements = [
#     # TODO(scotthavens): put setup requirements (distutils extensions, etc.) here
# ]

# test_requirements = [
#     # TODO: put package test requirements here
# ]

setup(
    name='weather_forecast_retrieval',
    version='0.6.11',
    description="Weather forecast retrieval gathers relevant gridded weather forecasts to ingest into physically based models for water supply forecasts",
    long_description=readme,
    long_description_content_type="text/markdown",
    author="Scott Havens",
    author_email='scott.havens@ars.usda.gov',
    url='https://github.com/USDA-ARS-NWRC/weather_forecast_retrieval',
    packages=['weather_forecast_retrieval'],
    entry_points={
        'console_scripts': [
            'grib2nc=weather_forecast_retrieval.grib2nc:main',
            'get_hrrr_archive=weather_forecast_retrieval.hrrr_archive:cli'
        ]},
    include_package_data=True,
    install_requires=requirements,
    license="CC0 1.0",
    zip_safe=False,
    keywords='weather_forecast_retrieval',
    classifiers=[
        'Development Status :: 4 - Beta',
        'Intended Audience :: Developers',
        'Natural Language :: English',
        'Programming Language :: Python :: 3.6',
        'Programming Language :: Python :: 3.7',
    ],
    test_suite='tests',
    scripts=[
        'scripts/run_hrrr_retrieval',
        'scripts/run_hrrr_retrieval_dates',
        'scripts/convert_grib2nc'
    ]
)

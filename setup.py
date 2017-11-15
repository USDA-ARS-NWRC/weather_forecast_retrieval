#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""The setup script."""

from setuptools import setup, find_packages

with open('README.rst') as readme_file:
    readme = readme_file.read()

with open('HISTORY.rst') as history_file:
    history = history_file.read()

requirements = [
    'Click>=6.0',
    # TODO: put package requirements here
]

setup_requirements = [
    # TODO(scotthavens): put setup requirements (distutils extensions, etc.) here
]

test_requirements = [
    # TODO: put package test requirements here
]

setup(
    name='weather_forecast_retrieval',
    version='0.1.0',
    description="Weather forecast retrieval gathers relavant gridded weather forecasts to ingest into physically based models for water supply forecasts",
    long_description=readme + '\n\n' + history,
    author="Audrey Roy Greenfeld",
    author_email='Scott Havens: scott.havens@ars.usda.gov',
    url='https://github.com/scotthavens/weather_forecast_retrieval',
    packages=find_packages(include=['weather_forecast_retrieval']),
    entry_points={
        'console_scripts': [
            'weather_forecast_retrieval=weather_forecast_retrieval.cli:main'
        ]
    },
    include_package_data=True,
    install_requires=requirements,
    license="MIT license",
    zip_safe=False,
    keywords='weather_forecast_retrieval',
    classifiers=[
        'Development Status :: 2 - Pre-Alpha',
        'Intended Audience :: Developers',
        'License :: OSI Approved :: MIT License',
        'Natural Language :: English',
        "Programming Language :: Python :: 2",
        'Programming Language :: Python :: 2.6',
        'Programming Language :: Python :: 2.7',
        'Programming Language :: Python :: 3',
        'Programming Language :: Python :: 3.3',
        'Programming Language :: Python :: 3.4',
        'Programming Language :: Python :: 3.5',
    ],
    test_suite='tests',
    tests_require=test_requirements,
    setup_requires=setup_requirements,
)

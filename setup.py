from setuptools import setup

with open('README.md') as readme_file:
    readme = readme_file.read()

with open('requirements.txt') as req_file:
    requirements = req_file.read()

setup(
    name='weather_forecast_retrieval',
    description="Weather forecast retrieval gathers relevant gridded weather "
                "forecasts to ingest into physically based models for water "
                "supply forecasts",
    long_description=readme,
    long_description_content_type="text/markdown",
    author="USDA ARS NWRC",
    author_email='snow@ars.usda.gov',
    url='https://github.com/USDA-ARS-NWRC/weather_forecast_retrieval',
    packages=['weather_forecast_retrieval'],
    entry_points={
        'console_scripts': [
            'grib2nc=weather_forecast_retrieval.grib2nc:main',
            'get_hrrr_archive=weather_forecast_retrieval.hrrr_archive:cli',
            'hrrr_preprocessor=weather_forecast_retrieval.hrrr_preprocessor:cli',
            'hrrr_nomads=weather_forecast_retrieval.hrrr_nomads:cli'
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
        'License :: CC0 1.0 Universal (CC0 1.0) Public Domain Dedication',
        'Programming Language :: Python :: 3.6',
        'Programming Language :: Python :: 3.7',
        'Programming Language :: Python :: 3.8',
        'Programming Language :: Python :: 3.9',
    ],
    test_suite='tests',
    extras_require={
        'tests': [
            'mock',
        ],
    },
    use_scm_version={
        'local_scheme': 'node-and-date',
    },
    setup_requires=[
        'setuptools_scm'
    ],
    scripts=[
        'scripts/run_hrrr_retrieval',
        'scripts/run_hrrr_retrieval_dates',
        'scripts/convert_grib2nc'
    ]
)

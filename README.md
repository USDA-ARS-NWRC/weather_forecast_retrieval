# Weather Forecast Retrieval

[![GitHub version](https://badge.fury.io/gh/USDA-ARS-NWRC%2Fweather_forecast_retieval.svg)](https://badge.fury.io/gh/USDA-ARS-NWRC%2Fweather_forecast_retieval)


Weather forecast retrieval gathers relevant gridded weather forecasts to ingest into physically based models for water supply forecasts

Current atmospheric models implemented:
* [High Resolution Rapid Refresh (HRRR)](https://rapidrefresh.noaa.gov/hrrr/)
* [Rapid Refresh (RAP)](https://rapidrefresh.noaa.gov/)

## Install
Follow these command line instructions to install weather_forecast_retrieval on Ubuntu 16.04 or 18.04.

Refer to the SMRF install instructions for use of a virtual environment. Virtual
Environments are recommended for python and pip install procedures to keep system
clean and organized. Make sure your virtual environment is sourced before proceeding.


### Ubuntu 16.04
Install necessary system packages

```
sudo apt update
sudo apt install python3-dev python3-pip python3-tk
sudo apt install libgrib-api-tools libgrib-api-dev
```

Get package from github and install

```
curl -L https://github.com/USDA-ARS-NWRC/weather_forecast_retrieval/archive/v0.5.2.tar.gz | tar xz

python3 -m pip install pygrib==2.0.2

cd weather_forecast_retrieval-0.5.2/
python3 -m pip install -r requirements_dev.txt
python3 setup.py install
```

Check install
```
python3 setup.py test
```

### Ubuntu 18.04

Install necessary system packages

```
sudo apt update
sudo apt install python3-dev python3-pip python3-tk

sudo apt install libeccodes-dev libeccodes-tools
```

Get packages from github

```
curl -L https://github.com/USDA-ARS-NWRC/weather_forecast_retrieval/archive/v0.5.2.tar.gz | tar xz
curl -L https://github.com/jswhit/pygrib/archive/v2.0.4rel.tar.gz | tar xz
python3 -m pip install pyproj==1.9.5.1
```

Install packages
This requires copying the ```setup.cfg``` that is filled out from weather_forecast_retrieval
and moving it into pygrib. This file points to the installed eccodes libraries.
```
cd pygrib-2.0.4rel

cp ../weather_forecast_retrieval-0.5.2/setup.cfg.pygrib ./setup.cfg

python3 setup.py build
python3 setup.py install

cd ../weather_forecast_retrieval-0.5.2/
python3 -m pip install -r requirements_dev.txt
python3 setup.py install
```

### grib2nc

To use the grib2nc command/function you will have to have wgrib2 installed.

This is easiest done by following [NOAA instructions](https://www.cpc.ncep.noaa.gov/products/wesley/wgrib2/compile_questions.html).
After completing their instructions, make wgrib2 accessible by cd into the source code and
attempt to install it under your ~/bin with:

```bash
ln wgrib2/wgrib2 ~/bin/wgrib2
```

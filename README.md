# Weather Forecast Retrieval


[![GitHub version](https://badge.fury.io/gh/USDA-ARS-NWRC%2Fweather_forecast_retrieval.svg)](https://badge.fury.io/gh/USDA-ARS-NWRC%2Fweather_forecast_retrieval)

Weather forecast retrieval gathers relevant gridded weather forecasts to ingest into physically based models for water supply forecasts

Current atmospheric models implemented:
* [High Resolution Rapid Refresh (HRRR)](https://rapidrefresh.noaa.gov/hrrr/)
* [Rapid Refresh (RAP)](https://rapidrefresh.noaa.gov/)

## Install

```
pip install weather-forecast-retrieval
```

## System dependencies

### nccopy

`nccopy` is used during the conversion in `grib2nc`. To install the `netCDF-C` libraries that are specific for your system. See the instructions from [Unidata](https://www.unidata.ucar.edu/software/netcdf/docs/getting_and_building_netcdf.html#sec_get_pre_built)

### wgrib2

To use the `grib2nc` command/function you will have to have `wgrib2` installed on the host computer.

This is easiest done by following [NOAA instructions](https://www.cpc.ncep.noaa.gov/products/wesley/wgrib2/compile_questions.html).
After completing their instructions, make wgrib2 accessible by cd into the source code and
attempt to install it under your ~/bin with:

```bash
ln wgrib2/wgrib2 ~/bin/wgrib2
```

## Docker

The retrieval aspect of `weather_forecast_retieval` has been built into a Docker image based on the Python 3 Alpine linux image. This allows for a docker deployment to run and retrieve HRRR data and convert to netcdf if needed. To use, first build the image

```
docker build -t usdaarsnwrc/weather_forecast_retieval .
```

Grab a coffee as this has to compile `pandas` from source (10+ minutes of compile time). Once completed, modify or create a new `docker-compose.yml` and modify the volume attachments as necessary. There are 2 volumes to attach, a `data` drive mounted to `/data` and the config file folders at `/code/config`. To setup the download, the config file is passed to `docker-compose`:

```
docker-compose run weather_forecast_retrieval /code/config/hrrr.ini
```

# Command line usage

## get_hrrr_archive

```
usage: get_hrrr_archive [-h] -s START_DATE -e END_DATE -o SAVE_DIR
                        [-f FORECASTS]

Command line tool for downloading HRRR grib files from the University of Utah

optional arguments:
  -h, --help            show this help message and exit
  -s START_DATE, --start START_DATE
                        Datetime to start, ie 2018-07-22 12:00
  -e END_DATE, --end END_DATE
                        Datetime to end, ie 2018-07-22 13:00
  -o SAVE_DIR, --output SAVE_DIR
                        Path to save the downloaded files to
  -f FORECASTS, --forecasts FORECASTS
                        Number of forecasts to get

```

The following command line will download data for a single hour and output into the `~/Downloads` folder to the file `~/Downloads/hrrr.20180722/hrrr.t12z.wrfsfcf01.grib2`:

```
get_hrrr_archive -s '2018-07-22 12:00' -e '2018-07-22 12:10' -o tests/RME/output/
```

## hrrr_preprocessor

Use `hrrr_preprocessor` to make smaller files from a larger HRRR file. This will crop to a bounding box and extract the following variables:

- air temperature 2m (TMP:2 m)
- relative_humidity 2m (RH:2 m)
- wind_u 10m (UGRD:10 m)
- wind_v 10m (VGRD:10 m)
- precip_int surface (APCP: surface)
- short_wave surface (DSWRF: surface)
- elevation (HGT:surface)
- TCDC for entire atmosphere (for WindNinja)

```bash
usage: hrrr_preprocessor [-h] -o OUTPUT_DIR -s START_DATE -e END_DATE -f
                         FORECAST_HR --bbox BBOX [--verbose]
                         hrrr_dir

Crop HRRR files by a bounding box and extract only the necessary surface variables for running with AWSM. 

Example command:
$ hrrr_preprocessor -s '2019-10-01 00:00' -e '2019-10-01 02:00' -f 0 --bbox="-119,-118,37,38" -o /path/to/output --verbose /path/to/hrrr

positional arguments:
  hrrr_dir              Directory of HRRR files to use as input

optional arguments:
  -h, --help            show this help message and exit
  -o OUTPUT_DIR, --output_dir OUTPUT_DIR
                        Directory to write cropped HRRR files to
  -s START_DATE, --start START_DATE
                        Start date
  -e END_DATE, --end END_DATE
                        End date
  -f FORECAST_HR, --forecast_hr FORECAST_HR
                        Forecast hour
  -n NCPU, --ncpu NCPU  Number of CPUs for wgrib2, 0 (default) will use all
                        available
  --bbox BBOX           Bounding box as delimited string --bbox='longitude
                        left, longitude right, latitude bottom, latitude top'
  --verbose             increase logging verbosity
```

## convert_grib2nc

## run_hrrr_retrieval

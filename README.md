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

The retrieval aspect of `weather_forecast_retieval` has been built into a Docker image based Python 3.8. This allows for a docker deployment to run and retrieve HRRR data. The docker image can call any of the command line programs in `weather_forecast_retrieval`.

For example, to run `hrrr_nomads` with docker:

```
docker run --rm usdaarsnwrs/weather_forecast_retrieval hrrr_nomads -l 3 -f 0,1,2 --bbox="-119,-118,37,38" -o /path/to/output -p /path/to/crop/output
```

The paths to the output directories are internal to the docker image and the necessary volume mounts are needed.


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

## hrrr_nomads

The `hrrr_nomads` command line will download HRRR grib2 files from NOMADS. `hrrr_nomads`
will fetch either the latest 3 hours of files or files between a start and end date. Optionally,
specify the forecast hours to limit how many files are downloaded. If a bounding box and
additional preprocess path is specified, `hrrr_nomads` will crop the files to the variables
needed for running AWSM.

> **_NOTE:_** Requires `wgrib2` to be installed if cropping to a bounding box.

Example to download the latest 3 hours of data for files not found in the output directory,
with the `00`, `01` and `02` forecast hours, crop to a bounding box:

```
hrrr_nomads -l 3 -f 0,1,2 --bbox="-119,-118,37,38" -o /path/to/output -p /path/to/crop/output
```

Usage:

```
usage: hrrr_nomads [-h] -o OUTPUT_DIR [-n NUM_REQUESTS] [-s START_DATE]
                   [-e END_DATE] [-l LATEST] [-f FORECAST_HRS] [--bbox BBOX]
                   [-p OUTPUT_PATH] [--verbose] [--overwrite]

Download from NOMADS and/or crop HRRR files by a bounding box and extract only
the necessary surface variables for running with AWSM.

Example command to download the latest 3 hours and crop to a bounding box:
$ hrrr_nomads -f 0 --bbox="-119,-118,37,38" -o /path/to/output -p /path/to/crop/output --verbose

optional arguments:
  -h, --help            show this help message and exit
  -o OUTPUT_DIR, --output_dir OUTPUT_DIR
                        Directory to download HRRR files to
  -n NUM_REQUESTS, --num_requests NUM_REQUESTS
                        Number of concurrent requests, default 2
  -s START_DATE, --start START_DATE
                        Start date
  -e END_DATE, --end END_DATE
                        End date
  -l LATEST, --latest LATEST
                        Latest number of hours to download, defaults 3 hours
  -f FORECAST_HRS, --forecast_hrs FORECAST_HRS
                        Forecast hours, comma seperated list
  --bbox BBOX           Bounding box as delimited string --bbox='longitude
                        left, longitude right, latitude bottom, latitude top'
  -p OUTPUT_PATH, --preprocess_path OUTPUT_PATH
                        Directory to write preprocessed HRRR files
  --verbose             increase logging verbosity
  --overwrite           Download and overwrite existing HRRR files
```

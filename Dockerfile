# The purpose of this Docker image is to facilitate the download and
# conversion of the HRRR grib2 files. 
FROM python:3.6.8-alpine3.9

MAINTAINER Scott Havens

# Install and make wgrib2
ENV CC=gcc
ENV FC=gfortran
ENV USE_NETCDF3=0
ENV USE_NETCDF4=1

WORKDIR /code
RUN apk --no-cache --virtual .build-dependencies add build-base curl gfortran && \
    apk --no-cache add libgfortran libgomp && \
    curl ftp://ftp.cpc.ncep.noaa.gov/wd51we/wgrib2/wgrib2.tgz | tar xvz && \
    cd /code/grib2 && \
    wget https://support.hdfgroup.org/ftp/HDF5/releases/hdf5-1.10/hdf5-1.10.4/src/hdf5-1.10.4.tar.gz && \
    wget ftp://ftp.unidata.ucar.edu/pub/netcdf/netcdf-4.6.1.tar.gz && \
    make && \
    ln wgrib2/wgrib2 /usr/local/bin/wgrib2 && \
    rm *.tar.gz && \
    apk del .build-dependencies

# Add and build weather forecast retrival
# WORKDIR /app
ADD . /code/weather_forecast_retrieval

# Using pip:
# RUN python3 -m pip install -r requirements_dev.txt
# CMD ["python3", "-m", "weather_forecast_retrieval"]


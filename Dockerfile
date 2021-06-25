# Multistage build to build wgrib2 first
FROM python:3.8.10-buster as builder

WORKDIR /build/wgrib2

# build wgrib2, this takes a while
ENV CC gcc
ENV FC gfortran

RUN apt-get update \
    && apt-get install -y gfortran \
    && curl ftp://ftp.cpc.ncep.noaa.gov/wd51we/wgrib2/wgrib2.tgz | tar xvz \
    && cd /build/wgrib2/grib2 \
    && wget https://support.hdfgroup.org/ftp/HDF5/releases/hdf5-1.10/hdf5-1.10.4/src/hdf5-1.10.4.tar.gz \
    && wget ftp://ftp.unidata.ucar.edu/pub/netcdf/netcdf-c-4.7.3.tar.gz \
    && sed -i "s/USE_NETCDF4=0/USE_NETCDF4=1/" makefile \
    && sed -i "s/USE_NETCDF3=1/USE_NETCDF3=0/" makefile \
    && make \
    && cp wgrib2/wgrib2 /usr/local/bin/wgrib2

##############################################
# main image
##############################################
FROM python:3.8.10-slim-buster

COPY . /code
WORKDIR /code

COPY --from=builder /usr/local/bin/wgrib2 /usr/local/bin/wgrib2

# Add and build weather forecast retrival
RUN apt-get update -y \
    && apt-get install -y git libeccodes-tools libgfortran5 libgomp1 \
    && python3 -m pip install --no-cache-dir -r requirements.txt \
    && python3 setup.py install \
    && rm -rf /var/lib/apt/lists/*

VOLUME /data
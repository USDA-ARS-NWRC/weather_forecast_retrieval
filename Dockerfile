# The purpose of this Docker image is to facilitate the download and
# conversion of the HRRR grib2 files. 
FROM python:3.6.8-alpine3.9

# Install and make wgrib2
ENV CC gcc
ENV FC gfortran

WORKDIR /code
RUN apk --no-cache --virtual .build-dependencies add build-base curl gfortran cmake zlib-dev perl diffutils bash curl-dev m4 && \
    apk --no-cache add libgfortran libgomp libstdc++ && \
    curl ftp://ftp.cpc.ncep.noaa.gov/wd51we/wgrib2/wgrib2.tgz | tar xvz && \
    cd /code/grib2 && \
    wget https://support.hdfgroup.org/ftp/HDF5/releases/hdf5-1.10/hdf5-1.10.4/src/hdf5-1.10.4.tar.gz && \
    wget ftp://ftp.unidata.ucar.edu/pub/netcdf/netcdf-4.6.1.tar.gz && \
    sed -i "s/USE_NETCDF4=0/USE_NETCDF4=1/" makefile && \
    sed -i "s/USE_NETCDF3=1/USE_NETCDF3=0/" makefile && \
    make && \
    cp wgrib2/wgrib2 /usr/local/bin/wgrib2 && \
    make deep-clean && \
    rm *.tar.gz
    
# build the requirements first, CCFLAGS help reduce the size of pandas and numpy
COPY requirements_grib2nc.txt /code
RUN CFLAGS="-g0 -Wl,--strip-all" \
    python3 -m pip install --no-cache-dir --compile --global-option=build_ext -r /code/requirements_grib2nc.txt 

# Add the weather code
ADD . /code/weather_forecast_retrieval

# Add and build weather forecast retrival
RUN cd /code/weather_forecast_retrieval && \
    python3 setup.py install && \
    apk del .build-dependencies

VOLUME /data

# ENTRYPOINT ["/code/weather_forecast_retrieval/docker-entrypoint.sh"]
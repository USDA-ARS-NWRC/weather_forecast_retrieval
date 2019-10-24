# The purpose of this Docker image is to facilitate the download and
# conversion of the HRRR grib2 files. 
FROM python:3.6.8-alpine3.9

# Install and make wgrib2
ENV CC gcc
ENV FC gfortran
ENV CLASSPATH ".:/usr/local/bin/antlr.jar:$CLASSPATH"

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

# ncap2 needs antlr which is a java program
RUN apk add openjdk8-jre-base && \
    curl http://dust.ess.uci.edu/nco/antlr-2.7.7.tar.gz | tar xvz && \
    cd /code/antlr-2.7.7 && \
    export CLASSPATH=".:/usr/local/bin/antlr.jar:$CLASSPATH" && \
    ./configure --prefix=/usr/local --disable-examples && \
    make && \
    make test && \
    make install && \
    mv antlr.jar /usr/local/bin && \
    rm -rf /code/antlr-2.7.7

# install NCO
# There is something wrong with either a dependency or with nco as the hdf5 headers are
# compiled with 1.10.4 but installed version is 1.10.5, go figure, just suppress the
# warnings and hope for the best!
ENV HDF5_DISABLE_VERSION_CHECK 2
RUN echo "http://dl-cdn.alpinelinux.org/alpine/edge/testing" >> /etc/apk/repositories && \
    apk --no-cache --virtual .nco-dependencies add flex byacc && \
    apk --no-cache add netcdf netcdf-dev && \
    cd /code && \
    wget https://github.com/nco/nco/archive/4.7.2.tar.gz && \
    tar xvzf 4.7.2.tar.gz && \
    rm 4.7.2.tar.gz && \
    cd nco-4.7.2 && \
    ./configure --prefix=/usr/local && \
    make && \
    make install && \
    apk del .nco-dependencies && \
    rm -rf /code/nco-4.7.2

# Add the weather code
ADD . /code/weather_forecast_retrieval

# Add and build weather forecast retrival
RUN cd /code/weather_forecast_retrieval && \
    apk --no-cache add netcdf-utils hdf5 hdf5-dev libffi-dev && \
    CFLAGS="-g0 -Wl,--strip-all" \
    python3 -m pip install --no-cache-dir --compile --global-option=build_ext -r requirements.txt && \
    python3 setup.py install && \
    apk del .build-dependencies

VOLUME /data

# ENTRYPOINT ["/code/weather_forecast_retrieval/docker-entrypoint.sh"]
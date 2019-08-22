#!/bin/sh
set -e

rhr='python3 /code/weather_forecast_retrieval/scripts/run_hrrr_retrieval'

if [ $# -eq 0 ]; then
    echo "Must pass config file to weather_forecast_retrieval"
    exit 1

elif [[ "$1" == *.ini ]]; then
    echo "Run weather_forecast_retrieval with config file"
    umask 0002
    echo "$1"
    exec $rhr "$1"

else
    echo "$@"
    umask 0002
    exec "$@"
fi
import argparse
import os
import time
from subprocess import check_output

from weather_forecast_retrieval import utils


def grib2nc(f_hrrr, output=None, external_logger=None, chunk_x=45, chunk_y=45):
    """
    Converts grib files to netcdf using HRRR forecast data and the
    variables required by the SMRF.
    Uses the wgrib2 command to identify variable names and uses that to filter
    the output from the commandline.

    Args:
        f_hrrr: Path to a HRRR grib file
        output: Path to output the resulting netcdf
        external_logger: External logger is desired
        chunk_x: number of chunks for the x dimension
        chunk_y: number of chunks for the y dimension

    """
    start = time.time()

    if external_logger is None:
        log = utils.create_logger(__name__)
        # fmt = "%(levelname)s: %(msg)s"
        # log = logging.getLogger(__name__)
        # coloredlogs.install(logger=log, fmt=fmt)

        msg = "GRIB2NC Converter Utility"
        log.info(msg)
        log.info("=" * len(msg))
    else:
        log = external_logger

    log.info('Converting to netcdf: {}'.format(f_hrrr))

    # criteria dictionary for extracting variables, CASE MATTERS
    criteria = {
        'air_temp': {
            'wgrib2 keys': [":TMP Temperature", "2 m"]
        },
        'dew_point': {
            'wgrib2 keys': [":DPT", "2 m"]
        },
        'relative_humidity': {
            'wgrib2 keys': [":RH Relative Humidity", "2 m"]
        },
        'wind_u': {
            'wgrib2 keys': [":UGRD U-Component", "10 m"]
        },
        'wind_v': {
            'wgrib2 keys': [":VGRD V-Component", "10 m"]
        },
        'precip_int': {
            'wgrib2 keys': [":APCP Total Precipitation"]
        },
        'short_wave': {
            'wgrib2 keys': ['Downward Short-Wave Radiation Flux', ':surface']
        },
        'elevation': {
            'wgrib2 keys': ['Geopotential Height', ':surface']
        }
    }

    # No output file name used, use the original plus a new extension
    if output is None:
        output = ".".join(os.path.basename(f_hrrr).split(".")[0:-1]) + ".nc"
    temp_output = output + '.tmp'

    grib_vars = ""
    var_count = 0

    # Cycle through all the variables and export the grib var names
    for k, v in criteria.items():
        log.debug("Attempting to extract grib name for {} ".format(k))

        cmd = "wgrib2 -v {} ".format(f_hrrr)

        # Add all the search filters
        for kw in v["wgrib2 keys"]:
            cmd += '| egrep "({})" '.format(kw)
        # Run the command

        # cmd += " -netcdf {}".format(output)
        s = check_output(cmd, shell=True).decode('utf-8')
        # num_grib_var = len(s.split('\n'))
        # Check if we only identify one variable based on line returns
        return_count = len([True for c in s if c == '\n'])

        if return_count != 1:
            log.warning("Found multiple variable entries for keywords "
                        "associated with {}".format(k))
            var_count += return_count
        else:
            var_count += 1
        # Add the grib var name to our running string/list
        grib_vars += s

    log.debug(
        "Extracting {} variables and converting to netcdf..."
        .format(var_count))
    log.debug("Outputting to: {}".format(temp_output))

    # Using the var names we just collected run wgrib2 for netcdf conversion
    log.debug(grib_vars)
    log.info("Converting grib2 to netcdf4...".format(
        len(grib_vars), len(cmd.split('\n'))))
    cmd = 'echo "{}" | wgrib2 -nc4 -i {} -netcdf {}'.format(
        grib_vars, f_hrrr, temp_output)
    log.debug(cmd)
    s = check_output(cmd, shell=True)

    # Recast dimensions
    log.info("Reducing dimensional variables to type ints and floats.")

    cmd = ("ncap2 -O -s 'latitude=float(latitude);longitude=float(longitude);"
           "x=int(x);y=int(y);time=int(time)' {0} {0}").format(temp_output)
    log.debug(cmd)
    s = check_output(cmd, shell=True)

    # Add chunking
    log.info("Adding chunking")
    cmd = "nccopy -w -c time/1,x/{},y/{} {} {}".format(
        chunk_x, chunk_y, temp_output, output)
    log.debug(cmd)
    s = check_output(cmd, shell=True)

    # clean up the temp file
    os.remove(temp_output)

    log.info("Complete! Elapsed {:0.0f}s".format(time.time()-start))


def main():
    p = argparse.ArgumentParser(description="Command line tool for converting"
                                " HRRR grib files to netcdf using only the "
                                " variables we want.")

    p.add_argument(dest="hrrr",
                   help="Path to the HRRR file containing the variables "
                        "for SMRF")
    p.add_argument("-o", "--output",
                   dest="output",
                   required=False,
                   default=None,
                   help="Path to output the netcdf file if you don't want it "
                        "renamed the same as the hrrr file with a different "
                        "extension.")
    args = p.parse_args()
    grib2nc(args.hrrr, args.output)


if __name__ == "__main__":
    main()

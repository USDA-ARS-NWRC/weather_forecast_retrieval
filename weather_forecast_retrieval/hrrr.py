from weather_forecast_retrieval.data.hrrr.file_loader import FileLoader


class HRRR(FileLoader):
    """
        Stub for backwards compatibility.
        All previous contained functionality moved to module:
            weather_forecast_retrieval.data.hrrr
    """
    pass

    # def check_file_health(self, output_dir, start_date, end_date,
    #                       hours=range(23), forecasts=range(18),min_size=100):
    #     """
    #     Check the health of the downloaded hrrr files so that we can download
    #     bad files from U of U archive if something has gone wrong.

    #     Args:
    #         output_dir:     Location of HRRR files
    #         start_date:     date to start checking files
    #         end_date:       date to stop checking files
    #         hours:          hours within the day to check
    #         forecasts:      forecast hours within the day to check

    #     Returns:
    #         files:          list of file names that failed the tests
    #     """
    #     fmt_day = '%Y%m%d'
    #     sd = start_date.date()
    #     ed = end_date.date()
    #     # base pattern template
    #     dir_pattern = os.path.join(output_dir, 'hrrr.{}')
    #     file_pattern_all = 'hrrr.t*z.wrfsfcf*.grib2'
    #     file_pattern = 'hrrr.t{:02d}z.wrfsfcf{:02d}.grib2'
    #     # get a date range
    #     num_days = (ed-sd).days
    #     d_range = [timedelta(days=d) + sd for d in range(num_days)]

    #     # empty list for storing bad files
    #     small_hrrr = []
    #     missing_hrrr = []

    #     for dt in d_range:
    #         # check for files that are too small first
    #         dir_key = dir_pattern.format(dt.strftime(fmt_day))
    #         file_key = file_pattern_all
    #         too_small = health_check.check_min_file_size(dir_key, file_key,
    #                                                      min_size=min_size)
    #         # add bad files to list
    #         small_hrrr += too_small
    #         # check same dirs for missing files
    #         for hr in hours:
    #             for fx in forecasts:
    #                 file_key = file_pattern.format(hr, fx)
    #                 missing = health_check.check_missing_file(
    #                     dir_key, file_key)
    #                 missing_hrrr += missing

    #     # get rid of duplicates
    #     small_hrrr = list(set(small_hrrr))
    #     missing_hrrr = list(set(missing_hrrr))

    #     return small_hrrr, missing_hrrr

    # def fix_bad_files(self, start_date, end_date, out_dir, min_size=100,
    #                   hours=range(23), forecasts=range(18)):
    #     """
    #     Routine for checking the downloaded file health for some files in the
    #     past and attempting to fix the bad file

    #     Args:
    #         start_date:     start date datetime object for checking the files
    #         end_date:       end date datetime object for checking the files
    #         out_dir:        base directory where the HRRR files are stored

    #     """
    #     # get the bad files
    #     small_hrrr, missing_hrrr = self.check_file_health(out_dir,
    #                                                       start_date,
    #                                                       end_date,
    #                                                       min_size=min_size,
    #                                                       hours=hours,
    #                                                       forecasts=forecasts)

    #     if len(missing_hrrr) > 0:
    #         self._logger.info('going to fix missing hrrr')
    #         for fp_mh in missing_hrrr:
    #             self._logger.debug(fp_mh)
    #             print(os.path.basename(fp_mh))
    #             file_day = pd.to_datetime(os.path.dirname(fp_mh)[-8:])
    #             success = hrrr_archive.download_url(os.path.basename(fp_mh),
    #                                                 out_dir,
    #                                                 self._logger,
    #                                                 file_day)

    #         self._logger.info('Finished fixing missing files')

    #     # run through the files and try to fix them
    #     if len(small_hrrr) > 0:
    #         self._logger.info('\n\ngoing to fix small hrrr')
    #         for fp_sh in small_hrrr:
    #             self._logger.info(fp_sh)
    #             file_day = pd.to_datetime(os.path.dirname(fp_sh)[-8:])
    #             # remove and redownload the file
    #             os.remove(fp_sh)
    #             success = hrrr_archive.download_url(os.path.basename(fp_sh),
    #                                                 out_dir,
    #                                                 self._logger,
    #                                                 file_day)

    #             if not success:
    #                 self._logger.warn('Could not download')

    #         self._logger.info('Finished fixing files that were too small')

import os
import logging

import pytz
from xmrgprocessing.xmrg_results import xmrg_results
from xmrgprocessing.xmrgdatasaver.nexrad_data_saver import precipitation_saver
from datetime import datetime
from pandas import read_csv
from pathlib import Path

class nexrad_csv_saver(precipitation_saver):
    def __init__(self, output_filename: str, source_tz: str, destination_tz: str):
        self._logger = logging.getLogger()

        self._new_records_added = 0
        self._boundary_output_files = {}
        self._now_date_time = datetime.now()
        self._finalized_filenames = []
        self._output_filename = Path(output_filename)
        unsorted_file_name = Path(self._output_filename.parent ) / f"{self._output_filename.stem}_unsorted{self._output_filename.suffix}"
        self._src_tz = pytz.timezone(source_tz)
        self._dst_tz = pytz.timezone(destination_tz)
        self._output_file_obj = open(unsorted_file_name, "w")
        self._output_file_obj.write("Area, Start Time, End Time, Weighted Average\n")

    @property
    def new_records_added(self):
        return self._new_records_added

    @property
    def csv_filenames(self):
        return self._finalized_filenames

    def save(self, xmrg_results_data: xmrg_results):
        '''
        Saves the xmrg_results_data to the CSV output file.
        :param xmrg_results_data:
        :return:
        '''
        for boundary_name, boundary_results in xmrg_results_data.get_boundary_data():
            try:
                #COnvert to inches.
                avg = (boundary_results['weighted_average'] / 25.4)
                #outstring = "%s,%s,%s,%f\n" % (boundary_name,xmrg_results_data.datetime,xmrg_results_data.datetime,avg)
                #self._output_file_obj.write(outstring)
                utc_datetime = self._src_tz.localize(datetime.strptime(xmrg_results_data.datetime, "%Y-%m-%dT%H:%M:%S"))
                local_datetime = utc_datetime.astimezone(self._dst_tz).strftime("%Y-%m-%dT%H:%M:%S")
                self._output_file_obj.write(f"{boundary_name},{local_datetime},{local_datetime},{avg:0.6f}\n")
            except Exception as e:
                self._logger.exception(e)

    def finalize(self):
        """
        This function is for us to clean up before the script exits.
        :return:
        """
        self._output_file_obj.close()
        try:
            unsorted_filename = self._output_file_obj.name
            directory, filename = os.path.split(unsorted_filename)
            filename = filename.replace("_unsorted.csv", ".csv")
            self._logger.info(f"Sorting file file: {unsorted_filename} into file: {filename}")
            pd_df = read_csv(unsorted_filename,
                             dtype={
                                 "Area": str,
                                 " Start Time": str,
                                 " End Time": str,
                                 " Weighted Average": str
                             },
                             keep_default_na=False
                             )
            sorted_df = pd_df.sort_values(by=' Start Time')
            final_filename = os.path.join(directory, filename)
            sorted_df.to_csv(final_filename, index=False)
            self._finalized_filenames.append(final_filename)
            self._logger.info(f"Deleting temp file: {unsorted_filename}")
            os.remove(unsorted_filename)
        except Exception as e:
            self._logger.exception(e)


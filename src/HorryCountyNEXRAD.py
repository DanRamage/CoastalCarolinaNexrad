"""
Revisions
Date: 2012-09-10
Author: DWR
Changes: In object dateControlFile, we now work in epoch seconds. Had a bug in the test to see which 
reporting period we were in. Using epoch makes the testing more straight forward
"""
# !/usr/bin/python
import sys
import os
import configparser as ConfigParser
import optparse
import logging.config
import time
import traceback

from pytz import timezone
from datetime import datetime, timedelta
from pytz import timezone
import csv
from pathlib import Path
from KMLBoundaryParser import KMLHUCBoundaryParser
from CSVDataSaver import nexrad_csv_saver
from xmrgprocessing.xmrg_process import xmrg_process
from xmrgprocessing.xmrg_utilities import file_list_from_date_range, http_download_file
from xmrgprocessing.xmrgfileiterator.xmrg_file_iterator import xmrg_file_iterator
from xmrgprocessing.archive.nfs_mount_utils import check_mount_exists, mount_nfs
from uuid import uuid4

# Data Processing flags
WORKER_COUNT = 4
if __name__ == '__main__':
    start_time = time.time()

    logger = None

    parser = optparse.OptionParser()
    parser.add_option("--ConfigFile", dest="configFile",
                      help="INI file containing various parameters for processing.")
    parser.add_option("--DateToProcess", dest="date_to_process", default=None,
                      help="If provided, this is the date we process back from.")
    parser.add_option("--HoursToProcess", dest="hours_to_process", type="int", default=24,
                      help="If provided, this is the number of hours we process back.")

    (options, args) = parser.parse_args()

    configFile = ConfigParser.RawConfigParser()
    configFile.read(options.configFile)
    print(f"Configuration file loaded: {configFile}")
    try:
        logConfFile = configFile.get('logging', 'configFile')
        if (logConfFile):
            logging.config.fileConfig(logConfFile)
            logger = logging.getLogger("horrycnt_nexrad_proc_logger")
            logger.info("Session started")
    except Exception as e:
        traceback.print_exc()
    else:
        try:
            watershed_list = configFile.get("settings", "watersheds")
        except ConfigParser.Error as e:
            if (logger):
                logger.exception(e)
            sys.exit(-1)
        else:
            watershed_list = watershed_list.split(',')
            if options.date_to_process is None:
                end_date = datetime.now()
            else:
                end_date = datetime.strptime(options.date_to_process, "%Y-%m-%d")

            number_of_hours_to_process = options.hours_to_process
            '''
            utcTZ = timezone('UTC')  
            utcDate = utcTZ.localize(datetime.datetime.strptime(startTime, "%Y-%m-%dT%H:%M:%S"))
            estStartTime = (utcDate.astimezone(timezone('US/Eastern'))).strftime("%Y-%m-%dT%H:%M:%S")
            '''

            utcTZ = timezone('UTC')
            estTZ = timezone('US/Eastern')
            end_date = estTZ.localize(end_date)

            end_date = end_date.replace(hour=8, minute=0, second=0, microsecond=0)
            start_date = end_date - timedelta(hours=number_of_hours_to_process)

            #We operate in EST, nexrad files are in UTC.
            utc_end_date = end_date.astimezone(utcTZ)
            utc_start_date = start_date.astimezone(utcTZ)

            # DL the xmrg files
            xmrg_download_url = configFile.get("xmrg_archive", 'download_url')
            xmrg_download_directory = Path(configFile.get("xmrg_archive", 'download_directory'))
            xmrg_download_directory.mkdir(parents=True, exist_ok=True)
            xmrg_filename_list = file_list_from_date_range(utc_end_date, number_of_hours_to_process, 'gz')
            for filename in xmrg_filename_list:
                http_download_file(xmrg_download_url, filename, xmrg_download_directory)

            # Process the individual watersheds.
            for watershed in watershed_list:
                watershed_start_time = time.time()
                # Get the required ini settings.
                try:
                    logger.info("Processing watershed: %s." % (watershed))

                    # local_mount_point = configFile.get("xmrg_archive", 'local_mount_point')
                    # xmrg_source_directory = configFile.get("xmrg_archive", 'xmrg_source_directory')
                    # if not check_mount_exists(local_mount_point):
                    #  mount_nfs(mount_nfs)
                    # Directory where the NEXRAD XMRG data files are for processing.
                    nexrad_data_dir = Path(configFile.get(watershed, 'NexradDataDir'))
                    nexrad_data_dir.mkdir(parents=True, exist_ok=True)
                    # After processing, this flag specifies if we are to delete the XMRG file we just processed.
                    remove_raw_nexrad_files = (configFile.getboolean(watershed, 'RemoveRawDataFiles'))
                    # After processing, this flag specifies if we are to delete the XMRG file we just processed.
                    # removeCompressedNexradFiles = bool(configFile.get(watershed, 'RemoveCompressedDataFiles'))
                    # File, either KML or CSV that contains the polygon(s) defining the watershed.
                    watershed_polygon_src = configFile.get(watershed, 'AreaPolygonFile')
                    # If set, the data is output in inches, native is 100th of mm.
                    output_in_inches = (configFile.getboolean(watershed, 'output_in_inches'))
                    # While processing, this is the file the data is saved to. Each polygon in the watershed is processed, then
                    # the result stored here for each hour processed. When we are on a reporting day, this file is then processed
                    # per polygon and individual files created for each polygon.
                    output_directory = Path(configFile.get(watershed, 'OutputDirectory'))
                    output_directory.mkdir(parents=True, exist_ok=True)
                    outputFilename = configFile.get(watershed, 'OutputFile')

                    create_hourly_file = configFile.getboolean(watershed, 'create_hourly_file')

                    save_all_precip_values = configFile.getboolean(watershed, 'save_all_precip_values')
                except ConfigParser.Error as e:
                    if (logger):
                        logger.exception(e)
                else:
                    try:
                        start_summary_hour = configFile.get(watershed, 'startSummaryHour')
                        # Params that aren't required
                        importBBOX = configFile.get(watershed, 'ImportBBOX')
                    except ConfigParser.Error as e:
                        if (logger):
                            logger.exception(e)

                    logger.debug(f"start_date: {start_date}({utc_start_date}) "
                                 f"end_date: {end_date}({utc_end_date})")

                    boundary_parser = KMLHUCBoundaryParser(unique_id="1")
                    boundaries = boundary_parser.parse(filepath=watershed_polygon_src)

                    filename_subs = {"watershed": watershed,
                                     "start": start_date.strftime("%Y-%m-%dT%H_%M_%S"),
                                     "end": end_date.strftime("%Y-%m-%dT%H_%M_%S")}

                    filename = outputFilename % (filename_subs)

                    output_filename = os.path.join(output_directory, filename)
                    csv_saver = nexrad_csv_saver(output_filename, "UTC", "US/Eastern")
                    task_id = uuid4()
                    xmrg_iterator = xmrg_file_iterator(start_date=utc_start_date,
                                                       end_date=utc_end_date,
                                                       full_xmrg_path=xmrg_download_directory)
                    xmrg_proc = xmrg_process(
                        file_list_iterator=xmrg_iterator,
                        data_saver=csv_saver,
                        boundaries=boundaries,
                        worker_process_count=WORKER_COUNT,
                        unique_id=task_id,
                        source_file_working_directory=nexrad_data_dir,
                        output_directory=output_directory,
                        base_log_output_directory=output_directory,
                        results_directory=output_directory,
                        kml_output_directory=output_directory,
                        save_all_precip_values=save_all_precip_values,
                        delete_source_file=remove_raw_nexrad_files,
                        delete_compressed_source_file=remove_raw_nexrad_files)
                    xmrg_proc.process(start_date=utc_start_date, end_date=utc_end_date,
                                      base_xmrg_directory=xmrg_download_directory)

                    logger.info(f"Finished processing {watershed} in {time.time() - watershed_start_time} seconds.")

            logger.info(f"Processing completed in: {time.time() - start_time} seconds.")



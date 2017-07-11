#!/usr/bin/env python
# coding: utf-8
"""
file: 'parse.py'
description:
    This python script can take high-level requests to fetch and parse
    rawacf data from SuperDARN via Globus and using the 'backscatter' 
    library. 

    The methods contained herein can also be used externally to 
    perform these high-level processing functions.

author: David Fairbairn
date: July 6 2017
"""

import logging
import os

from datetime import datetime as dt
import numpy as np
import sqlite3
import argparse
import time

import backscatter 
import rawacf_utils as rut
from rawacf_utils import two_pad

SUBPROC_JOIN_TIMEOUT = 15
SHORT_SLEEP_INTERVAL = 0.1

BAD_RAWACFS_FILE = './bad_rawacfs.txt'
BAD_CPIDS_FILE = './bad_cpids.txt'
LOG_FILE = 'parse.log'

logging.basicConfig(level=logging.DEBUG,
    format='%(levelname)s %(asctime)s: %(message)s', 
    datefmt='%m/%d/%Y %I:%M:%S %p')

logFormatter = logging.Formatter('%(levelname)s %(asctime)s: %(message)s')
rootLogger = logging.getLogger()

fileHandler = logging.FileHandler("./{0}".format(LOG_FILE))
fileHandler.setFormatter(logFormatter)
rootLogger.addHandler(fileHandler)

# -----------------------------------------------------------------------------
#                           High-Level Methods 
# -----------------------------------------------------------------------------

def process_rawacfs_day(year, month, day, station_code=None, conn=None):
    """
    A function which fetches and processes rawacfs from a particular day
    into the sqlite3 database. Can optionally select a particular day.

    :param year:
    :param month:
    :param day: 
    [:param station_code:] 3-letter [string] for the station e.g. 'sas' for Saskatoon

    ** Note: it would've been ideal to take station ID parameters but I didn't
            know of a really quick and easy way to convert stid's and station codes
            without requiring an installation of e.g. davitpy **
    """
    import subprocess
    import calendar 

    if conn==None:
        conn = sqlite3.connect("superdarntimes.sqlite")

    # If a stid is given to function, then just grab that station's stuff
    all_stids = True if station_code is None else False

    # I. Run the globus connect process
    rut.globus_connect()

    # II. Fetch the files
    if all_stids:
        script_query = [rut.SYNC_SCRIPT_LOC,'-y', str(year), '-m',
            str(month), '-p', str(year)+"{:02d}".format(month)+"{:02d}".format(day)+"*", rut.ENDPOINT]
    else:
        # The case that we're looking at just one particular station
        script_query = [rut.SYNC_SCRIPT_LOC,'-y', str(year), '-m',
            str(month), '-p', str(year)+"{:02d}".format(month)+"{:02d}".format(day)+"*"+station_code, 
            rut.ENDPOINT]
    rut.globus_query(script_query)

    # III.
    # B. Parse the rawacf files, save their metadata in our DB
    parse_rawacf_folder(rut.ENDPOINT, conn=conn)
    logging.info("\t\tDone with parsing {0}-{1}-{2} rawacf data".format(
                 str(year), "{:02d}".format(month), "{:02d}".format(day)))
    conn.commit()

    # C. Clear the rawacf files that were fetched in this cycle
    try:
        rut.clear_endpoint()
        logging.info("\t\tDone with clearing {0}-{1}-{2} rawacf data".format(
                 str(year), "{:02d}".format(month), "{:02d}".format(day)))
    except subprocess.CalledProcessError:
        logging.error("\t\tUnable to remove files.")
    logging.info("Completed processing of requested day's rawacf data.")
 
def process_rawacfs_month(year, month, conn=sqlite3.connect("superdarntimes.sqlite")):
    """
    Takes starting month and year and ending month and year as arguments. Steps
    through each day in each year/month combo

    :param year: [int] indicating the year to look at
    :param month: [int] indicating the month to look at
    :param conn: [sqlite3 connection] to the database for saving to

    ** On Maxwell this has taken upwards of 14 hours to run for a given month **

    """
    import subprocess
    import calendar 

    # I. Run the globus connect process
    rut.globus_connect()

    logging.info("Beginning to process Rawacf logs... ")
    
    last_day = calendar.monthrange(year, month)[1]
    logging.info("Starting to analyze {0}-{1} files...".format(str(year), "{:02d}".format(month))) 

    # II. For each day in the month:
    for day in np.arange(1,last_day+1):

        logging.info("\tLooking at {0}-{1}-{2}".format(
                     str(year), "{:02d}".format(month), "{:02d}".format(day)))

        # A. First, grab the rawacfs via globus (and wait on it)
        script_query = [rut.SYNC_SCRIPT_LOC,'-y', str(year), '-m',
            str(month), '-p', str(year)+"{:02d}".format(month)+"{:02d}".format(day)+"*", rut.ENDPOINT]

        rut.globus_query(script_query)

        # B. Parse the rawacf files, save their metadata in our DB
        parse_rawacf_folder(rut.ENDPOINT, conn=conn)
        logging.info("\t\tDone with parsing {0}-{1}-{2} rawacf data".format(
                     str(year), "{:02d}".format(month), "{:02d}".format(day)))

        # C. Clear the rawacf files that were fetched in this cycle
        try:
            rut.clear_endpoint()
            logging.info("\t\tDone with clearing {0}-{1}-{2} rawacf data".format(
                     str(year), "{:02d}".format(month), "{:02d}".format(day)))
        except subprocess.CalledProcessError:
            logging.error("\t\tUnable to remove files.")

    logging.info("Completed processing of requested month's rawacf data.")
    return
        
def test_process_rawacfs(conn=sqlite3.connect("superdarntimes.sqlite")):
    """
    This method exists specifically to test whether everything's 
    configured properly to run the script to grab an entire month's data. 
    It does everything that process_rawacfs_month does, but only a little bit.

    :param conn: [sqlite3 connection] to the database
    """
    import subprocess

    # Test 1: Globus query
    rut.globus_connect()
    script_query = [rut.SYNC_SCRIPT_LOC,'-y', '2017', '-m',
        '02', '-p', '20170209.0*zho', rut.ENDPOINT]
    rut.globus_query(script_query)

    # Test 2: verify that we can parse this stuff
    parse_rawacf_folder(rut.ENDPOINT, conn=conn )
    logging.info("Done with parsing 2017-02-09 'sas' rawacf data")

    # Test 3: Clear the rawacf files that we fetched
    try:
        rut.clear_endpoint()
        logging.info("Successfully removed 2017-02-09 'sas' rawacf data")

    except subprocess.CalledProcessError:
        logging.error("\t\tUnable to remove files")

def process_file(fname):
    """
    Essentially a wrapper for using parse_file that handles some possible 
    exceptions. This function is only used if you call the script to just
    process a particular file. (so its efficiency isn't as critical fyi)
    
    :param f: file name including path.
    """
    # Start exception handler/write handler
    manager = mp.Manager()
    exc_msg_queue = manager.Queue()
    write_handler = mp.Process(target=exc_handler_func, args=( exc_msg_queue))
    write_handler.start()
    try:
        dummy_index = 1
        r = parse_file(folder, fil, dummy_index, exc_msg_queue)

    except backscatter.dmap.DmapDataError as e:
        err_str = "\t{0} File: {1}: Error reading dmap from stream - possible record" + \
                  " corruption. Skipping file."
        logging.error(err_str.format(index, fname))
        return

    except rut.InconsistentRawacfError as e:
        err_str = "\t{0} File {1}: Exception raised during process_experiment: {2}"
        logging.warning(err_str.format(index, fname, e))
    r.save_to_db() 
    return r

def parse_rawacf_folder(folder, conn=sqlite3.connect("superdarntimes.sqlite")):
    """
    Takes a path to a folder which contains of .rawacf files, parses them
    and inserts them into the database.

    :param folder: [str] indicating the path and name of a folder to read 
                    rawacf files from
    :param conn: [sqlite3 connection] to the database
    """
    import multiprocessing as mp
    import itertools
    assert(os.path.isdir(folder))
    cur = conn.cursor()
    logging.info("Acceptable path {0}. Analysis proceeding...".format(folder))

    processes = []

    # Start exception handler/write handler
    manager = mp.Manager()
    exc_msg_queue = manager.Queue()
    write_handler = mp.Process(target=exc_handler_func, args=( exc_msg_queue,))
    write_handler.start()  

    # Assemble a bundle of arguments for mp.pool to use 
    files = os.listdir(folder) 
    file_indices = np.arange(1, len(files)+1) 
    arg_bundle = itertools.izip(itertools.repeat(folder), files, file_indices,
                itertools.repeat(exc_msg_queue))
  
    # Set the pool to work
    logging.debug("Beginning a pool multiprocessing of the files...") 
    pool = mp.Pool()
    recs = pool.map(parse_file_wrapper, arg_bundle)
    logging.debug("Done with multiprocessing of files (supposedly)")
       
    write_handler.terminate() 

    num_uncounted = 0
    for rec in recs:
        if rec is not None:
            rec.save_to_db(cur)
            conn.commit()
        else:
            num_uncounted += 1
            logging.debug("Found an instance of a None record!")

    done_str = "Done with processing files in folder. {0} / {1} were saved to the database."
    logging.info(done_str.format(len(files) - num_uncounted, len(files))) 
    # Commit the database changes
    conn.commit()

def parse_file(path, fname, index, exc_msg_queue):
    """
    Takes an individual .rawacf file, tries opening it, tries using 
    backscatter to parse it, and if successful at this, constructs a 
    RawacfRecord object and returns it.

    :param path: [string] path to file
    :param fname: [string] name of rawacf file
    :param exc_msg_queue: [mp.Queue] for sending exception messages to
    [:param index:] [int] number of file in directory. Helpful for logging.

    :returns: A RawacfRecord constructed using RawacfRecord.record_from_dics
                on a list of dictionaries assembled using 'backscatter'.
    """
    # I. Open File / Read with Backscatter
    logging.info("{0} File: {1}".format(index, fname)) 
    try:
        if fname[-4:] == '.bz2':
            dics = rut.bz2_dic(path + '/' + fname)
        elif fname[-7:] == '.rawacf':
            dics = rut.acf_dic(path + '/' + fname)
        else:
            logging.info('\t{0} File {1} not used for dmap records.'.format(index, fname))
            return None

    except backscatter.dmap.DmapDataError as e:
        err_str = "\t{0} File: {1}: Error reading dmap from stream - possible record" + \
                  " corruption. Skipping file."
        logging.error(err_str.format(index, fname))
        # Tell the write handler to add this to the list of bad rawacf files
        exc_msg_queue.put((fname, e))
        return None

    except MemoryError as e:
        logging.error("\t{0} File: {1}: RAN OUT OF MEMORY.".format(index, fname))
        exc_msg_queue.put((fname, e))
        # 'Just do it again!'. I know its inelegant, but this occurs rarely...
        import time
        time.sleep(SHORT_SLEEP_INTERVAL)
        return parse_file(path, fname, index, exc_msg_queue)

    # II. Make rawacf record and check the data's okay     
    try:
        r = rut.RawacfRecord.record_from_dics(dics)
        if r.not_corrupt == False:
            err_str = 'Data inconsistency encountered in rawacf file.'.format(index, fname)
            raise rut.InconsistentRawacfError(err_str)
        logging.info('\t{0} File  {1}: File processed.'.format(index, fname))

    except rut.InconsistentRawacfError as e:
        err_str = "\t{0} File {1}: Exception raised during process_experiment: {2}"
        logging.warning(err_str.format(index, fname, e))
        # Tell the write handler to add this to the list of files with bad CPIDS 
        exc_msg_queue.put((fname, e))

    # III. Output record
    return r

def parse_file_wrapper(args):
    """
    Wrapper for parse_file that takes one argument only (each of which is a
    tuple of parse_file's arguments).
    
    :param args: tuple of the arguments destined for parse_file
    
    :returns: the output of parse_file: a [rawacf_utils.RawacfRecord object]
    """
    return parse_file(*args)
  
def exc_handler_func(exc_msg_queue):
    """
    Function for doing the writing to bad_rawacfs.txt and bad_cpids.txt to 
    avoid race conditions between worker processes.

    :param exc_msg_queue: [multiprocessing.Queue] that provides a medium
                        for processes to send (rawacf_filename, exception)
                        tuples to handler for printing.
    """
    while True:
        if exc_msg_queue.empty():
            time.sleep(SHORT_SLEEP_INTERVAL)
        else:
            try:
                logging.debug("Write handler received a message!")
                fname, exc = exc_msg_queue.get()
                if type(exc) == rut.InconsistentRawacfError:
                    logging.debug("Write handler saving a bad_cpid event")
                    write_inconsistent_rawacf(fname, exc)
                elif type(exc) == backscatter.dmap.DmapDataError:
                    logging.debug("Write handler saving a bad_rawacf event")
                    write_bad_rawacf(fname, exc)
                elif type(exc) == MemoryError:
                    logging.debug("Exception handler sees memory error")
                    # TODO: Find a way to restart the process that has the memory error
            except TypeError:
                logging.error("Write handler had trouble unpacking message!")
            except IOError:
                logging.error("Write handler had trouble writing!")

def write_inconsistent_rawacf(fname, exc):
    """
    Performs the actual writing to the bad_cpids.txt file.

    :param fname: [str] filename that had inconsistent fields in it
    :param exc: [rawacf_utils.InconsistentRawacfError] exception object
    """
    # ***ADD TO LIST OF BAD_CPIDS ***
    with open(BAD_CPIDS_FILE, 'a') as f:
        f.write(fname + ':' + str(exc) + '\n')

def write_bad_rawacf(fname, exc): 
    """
    Performs the actual writing to the bad_rawacfs.txt file.

    :param fname: [str] filename that couldn't be opened by backscatter 
    :param exc: [backscatter.dmap.DmapDataError] exception object
   
    """
    # ***ADD TO LIST OF BAD_RAWACFS ***
    with open(BAD_RAWACFS_FILE, 'a') as f:
        # Backscatter exceptions have a newline that looks bad in 
        # logs, so I remove them here
        exc_tmp = str(exc).split('\n')
        exc_tmp = reduce(lambda x, y: x+y, exc_tmp)
        f.write(fname + ':"' + str(exc_tmp) + '"\n')
 
#------------------------------------------------------------------------------ 
#                       Command-Line Usability
#------------------------------------------------------------------------------ 

def get_args():
    """
    Parse the command-line arguments.

    Yes, in an ideal world, this whole thing would be a sweet little 
    object which does things on initialization, but at least for now,
    this works as a stand-alone function!
    """
    parser = argparse.ArgumentParser()
    parser.add_argument("-y", "--rec_year", help="Year you wish to process records for",
                        type=int)
    parser.add_argument("-m", "--rec_month", help="Month you wish to process records for",
                        type=int)
    parser.add_argument("-d", "--rec_day", help="Day you wish to process records for",
                        type=int)
    parser.add_argument("-p", "--directory", help="Indicate a directory to parse rawacfs in")

    parser.add_argument("-f", "--fname", help="Indicate a filename to process")

    # For now, we require a particular station to be requested
    parser.add_argument("-c", "--station_code", 
                        help="SuperDARN Station Code you want stats for (e.g. 'sas')")

    parser.add_argument("-q", "--quiet", help="Use quiet mode",
                        action="store_true")
    args = parser.parse_args()
    return args

def process_args(year, month, day, st_code, directory, fname):
    """
    Function which handles interpreting what kind of processing request
    to make.
    """
    # Highest precedence: if a particular file is provided as an arg.
    if fname is not None:
        if os.path.isfile(fname):
            logging.info("Parsing file {0}".format(fname))
            process_file(fname)
            return
        else:
            logging.error("Invalid filename.")

    # Next level of precedence: if a directory is supplied
    if directory is not None:
        if os.path.isdir(directory): 
            logging.info("Parsing files in directory {0}".format(directory))
            #parse_rawacf_folder(directory)
            return
        else:
            logging.error("Invalid directory.")

    if year is not None and month is not None:
        # Next check if a day was provided
        if day is not None:
            msg = "Proceeding to fetch and parse data from {0}-{1}-{2}"
            logging.info(msg.format(year, month, day))
            logging.info("By the way, station code supplied to this was: '{0}'".format(st_code))
            #process_rawacfs_day(year, month, day, station_code=st_code)
            return
        else:
            msg = "Proceeding to fetch and parse data in {0}-{1}"
            logging.info(msg.format(year, month))
            #process_rawacfs_month(year, month)
            return
    else:
        logging.info("Some form of argument is kinda required!")

def initialize_logger(quiet_mode):
    """
    Function for setting up the initial logging parameters

    :param use_verbose: [boolean] flag indicating whether to be verbose.
        ** If _not_ running parse/fetch requests from the command-line **
    """
    level = logging.WARNING if quiet_mode else logging.DEBUG

    logging.basicConfig(level=level,
        format='%(levelname)s %(asctime)s: %(message)s', 
        datefmt='%m/%d/%Y %I:%M:%S %p')
 
    logFormatter = logging.Formatter('%(levelname)s %(asctime)s: %(message)s')
    rootLogger = logging.getLogger()
    rootLogger.setLevel(level)

    fileHandler = logging.FileHandler("./{0}".format(LOG_FILE))
    fileHandler.setFormatter(logFormatter)
    rootLogger.addHandler(fileHandler)

#------------------------------------------------------------------------------ 

if __name__ == "__main__":
    args = get_args()

    year = args.rec_year
    month = args.rec_month
    day = args.rec_day
    st_code = args.station_code
    directory = args.directory
    fname = args.fname
    quietness_mode = args.quiet
    
    initialize_logger(quietness_mode)

    rut.read_config() 
    conn = rut.connect_db()
    cur = conn.cursor()
    process_args(year, month, day, st_code, directory, fname)
    

"""
file: 'uptime.py'
description:
    This file (currently) contains the methods that can be used to 
    calculate SuperDARN experiment statistics by manipulating the
    sqlite database constructed using methods from 
    'rawacf_metadata.py'.

author: David Fairbairn
date: June 20 2017

"""
import logging
import os
import sys

import dateutil.parser
from datetime import datetime as dt
import numpy as np
import sqlite3

import backscatter 
import rawacf_metadata as rmet
from rawacf_metadata import two_pad

BAD_RAWACFS_FILE = './bad_rawacfs.txt'
BAD_CPIDS_FILE = './bad_cpids.txt'
LOG_FILE = 'uptime.log'
SEC_IN_DAY = 86400.0

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
    all_stids = True if station_code == None else False

    # I. Run the globus connect process
    rmet.globus_connect()

    # II. Fetch the files
    if all_stids:
        script_query = [rmet.SYNC_SCRIPT_LOC,'-y', str(year), '-m',
            str(month), '-p', str(year)+two_pad(month)+two_pad(day)+"*", rmet.ENDPOINT]
    else:
        # The case that we're looking at just one particular station
        script_query = [rmet.SYNC_SCRIPT_LOC,'-y', str(year), '-m',
            str(month), '-p', str(year)+two_pad(month)+two_pad(day)+"*"+station_code, 
            rmet.ENDPOINT]
    rmet.globus_query(script_query)

    # III.
    # B. Parse the rawacf files, save their metadata in our DB
    parse_rawacf_folder(rmet.ENDPOINT, conn=conn)
    logging.info("\t\tDone with parsing {0}-{1}-{2} rawacf data".format(
                 str(year), two_pad(month), two_pad(day)))
    conn.commit()

    # C. Clear the rawacf files that were fetched in this cycle
    try:
        rmet.clear_endpoint()
        logging.info("\t\tDone with clearing {0}-{1}-{2} rawacf data".format(
                 str(year), two_pad(month), two_pad(day)))
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
    import rawacf_metadata as rmet

    # I. Run the globus connect process
    rmet.globus_connect()

    logging.info("Beginning to process Rawacf logs... ")
    
    last_day = calendar.monthrange(year, month)[1]
    logging.info("Starting to analyze {0}-{1} files...".format(str(year), two_pad(month))) 

    # II. For each day in the month:
    for day in np.arange(1,last_day+1):

        logging.info("\tLooking at {0}-{1}-{2}".format(
                     str(year), two_pad(month), two_pad(day)))

        # A. First, grab the rawacfs via globus (and wait on it)
        script_query = [rmet.SYNC_SCRIPT_LOC,'-y', str(year), '-m',
            str(month), '-p', str(year)+two_pad(month)+two_pad(day)+"*", rmet.ENDPOINT]

        rmet.globus_query(script_query)

        # B. Parse the rawacf files, save their metadata in our DB
        parse_rawacf_folder(rmet.ENDPOINT, conn=conn)
        logging.info("\t\tDone with parsing {0}-{1}-{2} rawacf data".format(
                     str(year), two_pad(month), two_pad(day)))

        # C. Clear the rawacf files that were fetched in this cycle
        try:
            rmet.clear_endpoint()
            logging.info("\t\tDone with clearing {0}-{1}-{2} rawacf data".format(
                     str(year), two_pad(month), two_pad(day)))
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
    rmet.globus_connect()
    script_query = [rmet.SYNC_SCRIPT_LOC,'-y', '2017', '-m',
        '02', '-p', '20170209.0*sas', rmet.ENDPOINT]
    rmet.globus_query(script_query)

    # Test 2: verify that we can parse this stuff
    parse_rawacf_folder(rmet.ENDPOINT, conn=conn )
    logging.info("Done with parsing 2017-02-09 'sas' rawacf data")

    # Test 3: Clear the rawacf files that we fetched
    try:
        rmet.clear_endpoint()
        logging.info("Successfully removed 2017-02-09 'sas' rawacf data")

    except subprocess.CalledProcessError:
        logging.error("\t\tUnable to remove files")
 
def parse_rawacf_folder(folder, conn=sqlite3.connect("superdarntimes.sqlite")):
    """
    Takes a path to a folder which contains of .rawacf files, parses them
    and inserts them into the database.

    :param folder: [str] indicating the path and name of a folder to read 
                    rawacf files from
    :param conn: [sqlite3 connection] to the database
    """
    import multiprocessing as mp
    assert(os.path.isdir(folder))
    cur = conn.cursor()
    logging.info("Acceptable path {0}. Analysis proceeding...".format(folder))

    #pool = mp.Pool()
    #pool.map(parse_file, os.listdir(folder)) # missing folder and index args 

    processes = []
    for i, fil in enumerate(os.listdir(folder)):
        p = mp.Process(target=parse_file, args=(folder, fil, i,))
        p.start()
        processes.append(p)
        #parse_file(folder, fil, index=i)

    for p in processes:
        p.join()

    # Commit the database changes
    conn.commit()

def parse_file(path, fname, index=1):
    """
    Takes an individual .rawacf file, tries opening it, tries using 
    backscatter to parse it, and tries processing the data in to the 
    sqlite database.

    :param path: [string] path to file
    :param fname: [string] name of rawacf file
    [:param index:] [int] number of file in directory. Helpful for logging.
    """
    logging.info("{0} File: {1}".format(index, fname)) 
    try:
        if fname[-4:] == '.bz2':
            dics = rmet.bz2_dic(path + '/' + fname)
        elif fil[-7:] == '.rawacf':
            dics = rmet.acf_dic(path + '/' + fname)
        else:
            logging.info('\t{0} File {1} not used for dmap records.'.format(index, fname))
            return
    except backscatter.dmap.DmapDataError as e:
        err_str = "\t{0} File: {1}: Error reading dmap from stream - possible record" + \
                  " corruption. Skipping file."
        logging.error(err_str.format(index, fname))
        #logging.exception(e)

        # ***ADD TO LIST OF BAD_RAWACFS ***
        with open(BAD_RAWACFS_FILE, 'a') as f:
            # Backscatter exceptions have an awkward newline that looks bad in 
            # logs, so I remove them here
            e_tmp = str(e).split('\n')
            e_tmp = reduce(lambda x, y: x+y, e_tmp)
            f.write(fname + ':"' + str(e_tmp) + '"\n')
        return
     
    # If it was a bz2 or rawacf, now we do scripty stuff with dict
    try:
        r = rmet.process_experiment(dics, conn)
        conn.commit()
        if r.not_corrupt == False:
            err_str = '\t{0} File: {1}: Data anomaly detected.'.format(index, fname)
            raise rmet.BadRawacfDataError(err_str)
        # Else, log the successful processing
        logging.info('\t{0} File  {1}: File processed.'.format(index, fname))
    except Exception as e:
        err_str = "\t{0} File {1}: Exception raised during process_experiment: {2}"
        logging.error(err_str.format(index, fname, e))
        #logging.exception(e)
        # ***ADD TO LIST OF BAD_RAWACFS ***
        with open(BAD_CPIDS_FILE, 'a') as f:
            f.write(fname + ':' + str(e) + '\n')


# -----------------------------------------------------------------------------
#                           POST PROCESSING METHODS
# -----------------------------------------------------------------------------

def stats_day(date_str, cur, stid=5):
    """
    Performs a database query, reporting the % usage of a SuperDARN 
    array on a particular day. 

    *** PARAMS ***
        date_str (string): a string in the format "20160418" yyyymmdd
        cur (sqlite3 cursor): cursor into an sqlite3 database
        stid (int): the station ID for the SuperDARN array desired
                see here: http://superdarn.ca/news/item/58-sd-radar-list
    """
    assert(len(date_str)==8 and str.isdigit(date_str))
    date_str_iso = date_str[:4] + "-" + date_str[4:6] + "-" + date_str[-2:]

    uptime_on_day = dict()
    sql = "select * from exps where (start_iso like '%{0}%' or end_iso like '%{1}%') and stid='{2}'"
    recs = rmet.select_exps(sql.format(date_str_iso, date_str_iso, stid), cur)

    for r in recs:
        logging.debug("Looking at record from {0} to {1}".format(r.start_dt, r.end_dt))
        st = r.start_dt
        et = r.end_dt
        if rmet.get_datestr(st) != rmet.get_datestr(et):
            logging.debug("\tStart date not the same as end date?")
            if date_str == rmet.get_datestr(st):
                logging.debug("\tSpecial case: end-of-day record")
                # Special case of end-of-day record
                seconds_this_day = SEC_IN_DAY - rmet.get_tod_seconds(st)
            elif date_str == rmet.get_datestr(et):
                logging.debug("\tSpecial case: start-of-day record")
                # Special case of start-of-day record
                seconds_prev_day = SEC_IN_DAY - rmet.get_tod_seconds(st)
                seconds_this_day = r.duration() - seconds_prev_day                
            else: 
                # This shouldn't ever happen
                raise Exception('Unexpected start date and end date discrepancies?')
        else:
            seconds_this_day = r.duration()
        logging.debug("Seconds of operation for this record: {0}\n".format(seconds_this_day)) 
        uptime_on_day[r] = seconds_this_day 
    
    uptime_pct = sum(uptime_on_day.values())/SEC_IN_DAY
    return uptime_pct

def stats_month(year, month, cur, stid=5):
    """
    Calculates uptime stats for the entire month 
    """
    import calendar    
    last_day = calendar.monthrange(year, month)[1]
    days = map(rmet.two_pad, np.arange(1, last_day + 1))
    day_stats = []
    for d in days:
        date_str = str(year) + rmet.two_pad(month) + d
        day_stats.append(stats_day(date_str, cur, stid)) 
    return day_stats

if __name__ == "__main__":
    rmet.read_config() 
    conn = rmet.connect_db()
    cur = conn.cursor()
    cur.execute('select * from exps')
    tup = cur.fetchall()[0]

    print("Now creating RawacfRecord files")
    # Test RawacfRecord's class method constructors
    r2 = rmet.RawacfRecord.record_from_tuple(tup)
    t = rmet.get_tod_seconds(r2.start_dt)



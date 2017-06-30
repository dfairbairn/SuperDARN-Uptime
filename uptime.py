"""
file: 'uptime.py'
description:
    This file (currently) contains the methods that can be used to 
    calculate SuperDARN experiment statistics by manipulating the
    sqlite database constructed using methods from 
    'rawacf_metadata.py'.

    Ultimately, this file will work as a front-end for users, so 
    high-level methods to fetch and process data will be moved into
    here to sit alongside the statistics methods. 

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

logging.basicConfig(level=logging.DEBUG,
    format='%(levelname)s %(asctime)s: %(message)s', 
    datefmt='%m/%d/%Y %I:%M:%S %p')

# -----------------------------------------------------------------------------
#                           High-Level Methods 
# -----------------------------------------------------------------------------

def process_rawacfs_dates(start_month, start_year, end_month, end_year):
    """
    The giant method that could be run which would repeatedly call 
    process_rawacfs_month (might do this using a bash script though)

    (This function hasn't been tested yet and would probably take days)
    """
    import subprocess
    import calendar 

    start = str(start_year) + two_pad(start_month)
    end = str(end_year) + two_pad(end_month)
    logname = 'process_rawacf_{0}_{1}.log'.format(start, end)
    logging.basicConfig(filename=logname, level=logging.INFO)

    # I. Run the globus connect process
    _ = subprocess.check_output([rmet.GLOBUS_STARTUP_LOC, '-start', '&'])

    # II. For each yr, month:
    for yr, mo in month_year_iterator(start_month, start_year, end_month, end_year):
        process_rawacfs_month(yr, mo)
 
def process_rawacfs_month(yr, mo, conn=sqlite3.connect("superdarntimes.sqlite")):
    """
    Takes starting month and year and ending month and year as arguments. Steps
    through each day in each year/month combo

    :param yr: [int] indicating the year to look at
    :param mo: [int] indicating the month to look at
    :param conn: [sqlite3 connection] to the database for saving to

    """
    import subprocess
    import calendar 
    import rawacf_metadata as rmet

    date = str(yr) + two_pad(mo)
    logname = 'process_rawacf_{0}.log'.format(date)
    logging.basicConfig(filename=logname, level=logging.INFO)

    # I. Run the globus connect process
    _ = subprocess.check_output([rmet.GLOBUS_STARTUP_LOC, '-start', '&'])
    #   TODO: If startup unsuccessful, check if its bc its already running

    logging.info("Beginning to process Rawacf logs... ")
    
    last_day = calendar.monthrange(yr, mo)[1]
    logging.info("Starting to analyze {0}-{1} files...".format(str(yr), two_pad(mo))) 

    # II. For each day in the month:
    for dy in np.arange(1,last_day):
        # Premature completion of script for debugging purposes 29-june-2017
        if dy > 1:
            logging.info("Completed subset of requested month's rawacf processing.")
            return

        logging.info("\tLooking at {0}-{1}-{2}".format(
                     str(yr), two_pad(mo), two_pad(dy)))
        # A. First, grab the rawacfs via globus (and wait on it)
        script_query = [rmet.SYNC_SCRIPT_LOC,'-y', str(yr), '-m',
            str(mo), '-p', str(yr)+two_pad(mo)+two_pad(dy)+"*", rmet.ENDPOINT]
        logging.info("\t\tPreparing to query: {0}".format(script_query))
        try:
            fetch = subprocess.check_output(script_query)
            logging.info("\t\tFetch request answered with: {0}".format(fetch))
        except subprocess.CalledProcessError:
            logging.error("\t\tFailed Globus query.")
        except OSError:
            logging.error("\t\tFailed to call Globus script")

        # B. Parse the rawacf files, save their metadata in our DB
        parse_rawacf_folder(rmet.ENDPOINT, conn=conn)
        logging.info("\t\tDone with parsing {0}-{1}-{2} rawacf data".format(
                     str(yr), two_pad(mo), two_pad(dy)))

        # C. Clear the rawacf files that were fetched in this cycle
        try:
            rmet.clear_endpoint()
            logging.info("\t\tDone with clearing {0}-{1}-{2} rawacf data".format(
                     str(yr), two_pad(mo), two_pad(dy)))
        except subprocess.CalledProcessError:
            logging.error("\t\tUnable to remove files.")
        
def test_process_rawacfs(conn=sqlite3.connect("superdarntimes.sqlite")):
    """
    This method exists specifically to test whether everything's 
    configured properly to run the script to grab an entire month or 
    year's data. 
    It does everything that process_rawacfs_month does, but only a little bit.

    :param conn: [sqlite3 connection] to the database
    """
    import subprocess
    # Test 1: running globusconnect 
    _ = subprocess.check_output([rmet.GLOBUS_STARTUP_LOC, '-start', '&'])

    # Test 2: perform a globus fetch using the script
    script_query = [rmet.SYNC_SCRIPT_LOC,'-y', '2017', '-m',
        '02', '-p', '20170209.02*sas', rmet.ENDPOINT]
    logging.info("Preparing to query: {0}".format(script_query))
    try:
        fetch = subprocess.check_output(script_query)
        logging.info("Fetch request answered with: {0}".format(fetch))
    except subprocess.CalledProcessError:
        logging.error("\t\tFailed Globus query.")
    except OSError:
        logging.error("\t\tFailed to call Globus script")
 
    # Test 3: verify that we can parse this stuff
    parse_rawacf_folder(rmet.ENDPOINT, conn=conn )
    logging.info("Done with parsing 2017-02-09 'sas' rawacf data")

    # Test 4: Clear the rawacf files that we fetched
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
    # For now just leave this empty; the stuff in if-main will go here
    assert(os.path.isdir(folder))
    cur = conn.cursor()
    logging.info("Acceptable path {0}. Analysis proceeding...".format(folder))
    for i, fil in enumerate(os.listdir(folder)):
        logging.info("{0} File: {1}".format(i, fil)) 
        try:
            if fil[-4:] == '.bz2':
                dics = rmet.bz2_dic(folder + '/' + fil)
            elif fil[-7:] == '.rawacf':
                dics = rmet.acf_dic(folder + '/' + fil)
            else:
                # Could do something with these too?
                logging.info('\tFile {0} not used for dmap records.'.format(fil))
                continue
        except backscatter.dmap.DmapDataError:
            err_str = "Error reading dmap frmo stream - possible record" + \
                      " corruption. Skipping file {0}"
            logging.error(err_str)
            continue
         
        # If it was a bz2 or rawacf, now we do scripty stuff with dict
        try:
            rmet.process_experiment(dics, cur, conn)
            logging.info('\tFile {0} processed.'.format(fil))
        except:
            logging.error("\tException raised during process_experiment.")

       # Commit the database changes
        conn.commit()

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

    # TODO: Finish this!

    import heapq
    sql = 'select * from exps where start_date regexp "%s" or end_date regexp "%s"'
    schedule = []
    rec_keys = dict()
    recs = select_exps(sql)
    for r in recs:
        start_tod = get_tod_seconds(r.start_dt)
        end_tod = get_tod_seconds(r.end_dt)
        rec_keys[start_tod] = r
        if r.start_date != date_str:
            heapq.heappush(schedule, 0.)
        else: 
            heapq.heappush(schedule, tod_sc)
    # Now we have a sorted list of the rawacf records for this day
    durations = []
    # Now use the 
    for start_sc in [heappop(schedule) for i in range(len(schedule))]:
        r = rec_keys[start_sc]
        if start_t_s == 0:
            # Then take the end
            #durations.append(r.end_time)
            pass
        else:
            pass
            #durations.append(r.end_time - r.start_time)
    # sum the intervals in durations and divide by 24hr*3600s/hr
    return None

def stats_month(date_str, cur, stid=5):
    """
    As above so below baby
    """
    return None

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



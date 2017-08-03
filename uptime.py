#!/usr/bin/env python
# coding: utf-8
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
import argparse

from datetime import datetime as dt
import numpy as np
import sqlite3
import calendar

import rawacf_utils as rut
from rawacf_utils import two_pad

LOG_FILE = 'uptime.log'
SEC_IN_DAY = 86400.0

# -----------------------------------------------------------------------------
#                           POST PROCESSING METHODS
# -----------------------------------------------------------------------------

def stats_day_summary(year, month, day, cur):
    """
    Return stats from all radars for this day, place in a results dictionary.
    """
    # No code specified, get stats for all radars recursively
    stats = dict()
    radar_codes = rut.allradars.keys()
    for code in radar_codes:
        stats[code] = stats_day(year, month, day, cur, code)
    return stats        

def stats_month_summary(year, month, cur):
    """
    Get stats from each radar's month, put means in a results dictionary and
    return an array.
    
    :param
    """
    #TODO: params
    stats = dict()
    averages = dict()
    radar_codes = rut.allradars.keys()
    for code in radar_codes:  
        stats_list = stats_month(year, month, cur, code)
        stats[code] = stats_list
        averages[code] = np.mean(stats_list)
    print(averages)
    return stats, averages

def do_forall_radars(func, arg_bundle):
    """
    Use this to return a dictionary of the results of stats_day or stats_month
    for all radar codes.
    
    :param func: [function handle] to a function to perform
    :param arg_bundle: [tuple] of the arguments that "func" needs OTHER THAN station code
    """
    stats = dict()
    radar_codes = rut.allradars.keys()
    for i, code in enumerate(radar_codes):  
        try:
            # Have faith that the caller gave us a good function/argbundle
            stat = func(*arg_bundle, code=code)
        except Exception as e:
            logging.exception(e)
            return None
        stats[code] = stat
        # Takes a while to sift through all radars' data in a month sometimes, 
        # so here I show a loading bar
        update_progress(float(i)/len(radar_codes))
    # newline to flush to the next line from the loading bar
    print('\n')
    return stats
 
def stats_day(year, month, day, cur, code=None):
    """
    Performs a database query, reporting the % usage of a SuperDARN 
    array on a particular day. 

    *** PARAMS ***
        date_str (string): a string in the format "20160418" yyyymmdd
        cur (sqlite3 cursor): cursor into an sqlite3 database
        stid (int): the station ID for the SuperDARN array desired
                see here: http://superdarn.ca/news/item/58-sd-radar-list
    """
    #TODO: params
    assert(year > 2002)
    assert(month in np.arange(1,13))
    last_day = calendar.monthrange(year, month)[1]
    assert(day in np.arange(1,last_day+1))
    if code is None:
        logging.warning("No station code given, proceeding with default, Saskatoon ('sas')")

    date_str = str(year) + two_pad(month) + two_pad(day)
    date_str_iso = str(year) + "-" + two_pad(month) + "-" + two_pad(day)

    uptime_on_day = dict()

    stid = rut.get_stid(code)
    sql = "select * from exps where (start_iso like '%{0}%' or end_iso like '%{1}%') and stid='{2}'"
    recs = rut.select_exps(sql.format(date_str_iso, date_str_iso, stid), cur)

    for r in recs:
        logging.debug("Looking at record from {0} to {1}".format(r.start_dt, r.end_dt))
        st = r.start_dt
        et = r.end_dt
        if rut.get_datestr(st) != rut.get_datestr(et):
            logging.debug("\tStart date not the same as end date?")
            if date_str == rut.get_datestr(st):
                logging.debug("\tSpecial case: end-of-day record")
                seconds_this_day = SEC_IN_DAY - rut.get_tod_seconds(st)
            elif date_str == rut.get_datestr(et):
                logging.debug("\tSpecial case: start-of-day record")
                seconds_prev_day = SEC_IN_DAY - rut.get_tod_seconds(st)
                seconds_this_day = r.duration() - seconds_prev_day                
            else: 
                # This shouldn't ever happen
                raise Exception('Unexpected start date and end date discrepancies?')
        else:
            seconds_this_day = r.duration()
        logging.debug("Seconds of operation for this record: {0}\n".format(seconds_this_day)) 
        uptime_on_day[r] = seconds_this_day 
    
    uptime_pct = sum(uptime_on_day.values())/SEC_IN_DAY * 100.
    return uptime_pct

def stats_month(year, month, cur, code=None):
    """
    Calculates uptime stats for the entire month 
    """
    if code is None:
        logging.warning("No station code given, proceeding with default, Saskatoon ('sas')")

    last_day = calendar.monthrange(year, month)[1]
    days = np.arange(1, last_day + 1)
    day_stats = []
    for day in days:
        day_stats.append(stats_day(year, month, day, cur, code)) 
    #print("{0}: {1} % Uptime".format(code, np.mean(day_stats)))
    return day_stats

def stats_summary(cur):
    """
    Informational overview of timespan of entries in DB
    """
    sql = "select * from exps"
    recs = rut.select_exps(sql, cur)
    if type(recs)!=list or len(recs)==0:
        logging.warning("No entries in database!")
    else:
        times = [ r.start_dt for r in recs ]
        first = min(times)
        last = max(times)
        print("Entries in database span {0} through {1}".format(first, last))
    return None

def update_progress(progress):
    """
    # update_progress() : Displays or updates a console progress bar
    Accepts a float between 0 and 1. Any int will be converted to a float.
    A value under 0 represents a 'halt'.
    A value at 1 or bigger represents 100%

    Code by Brian Khuu
    """
    import time, sys
    barLength = 10 # Modify this to change the length of the progress bar
    status = ""
    if isinstance(progress, int):
        progress = float(progress)
    if not isinstance(progress, float):
        progress = 0
        status = "error: progress var must be float\r\n"
    if progress < 0:
        progress = 0
        status = "Halt...\r\n"
    if progress >= 1:
        progress = 1
        status = "Done...\r\n"
    block = int(round(barLength*progress))
    text = "\rPercent: [{0}] {1}% {2}".format( "#"*block + "-"*(barLength-block), progress*100, status)
    sys.stdout.write(text)
    sys.stdout.flush()

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
    parser.add_argument("-y", "--stats_year", help="Year you wish to get stats for",
                        type=int)
    parser.add_argument("-m", "--stats_month", help="Month you wish to get stats for",
                        type=int)
    parser.add_argument("-d", "--stats_day", help="Day you wish to get stats for",
                        type=int) 

    # For now, we require a particular station to be requested
    parser.add_argument("-c", "--station_code", 
                        help="SuperDARN Station code you want stats for (e.g. sas)",
                        type=str)

    parser.add_argument("-v", "--verbose", help="Use verbose mode",
                        action="store_true")

    parser.add_argument("-f", "--db_file", help="Specified sqlite database to query",
                        type=str)

    args = parser.parse_args()
    year = args.stats_year
    month = args.stats_month
    day = args.stats_day
    st_code = args.station_code
    use_verbose = args.verbose
    db_file = args.db_file
    return (year, month, day, st_code, use_verbose, db_file)

def process_args(year, month, day, st_code, use_verbose, cur):
    """
    Encapsulates the necessary logic to decide what to do based on 
    command-line arguments.
    """
    if use_verbose:
        logging.info("Verbosity set high!")
        initialize_logger(use_verbose)
    else:
        logging.info("Verbosity set medium!")
        initialize_logger(use_verbose)

    if day is not None:
        if st_code is not None:
            stats = stats_day(year, month, day, cur, st_code)
        else:
            stats = do_forall_radars(stats_day, (year, month, day, cur))
#            stats = stats_day_summary(year, month, day, cur)
    elif month is not None:
        if st_code is not None:
            stats = stats_month(year, month, cur, st_code)
        else:
            stats = do_forall_radars(stats_month, (year, month, cur))
#            stats = stats_month_summary(year, month, cur)
    else:
        stats = stats_summary(cur)
    return stats

def initialize_logger(use_verbose):
    """
    Function for setting up the initial logging parameters

    :param use_verbose: [boolean] flag indicating whether to be verbose
    """
    level = logging.DEBUG if use_verbose else logging.INFO

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
    year, month, day, st_code, use_verbose, db_file = get_args()
    initialize_logger(use_verbose)        
    rut.read_config()
    if db_file is not None:
        logging.info("Going with specified database {0}".format(db_file))
        conn = rut.connect_db(db_file)
        cur = conn.cursor()     
    else:
        logging.info("Going with default database 'superdarntimes.sqlite'")
        conn = rut.connect_db()
        cur = conn.cursor()
    stats = process_args(year, month, day, st_code, use_verbose, cur)
    if stats is not None:  
        print("\nStatistics are shown below for selected period:")
        if type(stats)==dict: 
            for code in stats.keys():
                stat = stats[code]
                if type(stat)==list:
                    stat = np.mean(stat)
                print("{0}: {1} % Uptime".format(code, stat))
        else:
            print(stats)

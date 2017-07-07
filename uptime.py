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

def stats_day(year, month, day, cur, stid=5):
    """
    Performs a database query, reporting the % usage of a SuperDARN 
    array on a particular day. 

    *** PARAMS ***
        date_str (string): a string in the format "20160418" yyyymmdd
        cur (sqlite3 cursor): cursor into an sqlite3 database
        stid (int): the station ID for the SuperDARN array desired
                see here: http://superdarn.ca/news/item/58-sd-radar-list
    """
    assert(year > 2002)
    assert(month in np.arange(1,13))
    last_day = calendar.monthrange(year, month)[1]
    assert(day in np.arange(1,last_day+1))
    # assert Have data for this time

    date_str = str(year) + two_pad(month) + two_pad(day)
    date_str_iso = str(year) + "-" + two_pad(month) + "-" + two_pad(day)

    uptime_on_day = dict()
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
                # Special case of end-of-day record
                seconds_this_day = SEC_IN_DAY - rut.get_tod_seconds(st)
            elif date_str == rut.get_datestr(et):
                logging.debug("\tSpecial case: start-of-day record")
                # Special case of start-of-day record
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

def stats_month(year, month, cur, stid=5):
    """
    Calculates uptime stats for the entire month 
    """
    last_day = calendar.monthrange(year, month)[1]
    days = np.arange(1, last_day + 1)
    day_stats = []
    for day in days:
        day_stats.append(stats_day(year, month, day, cur, stid)) 
    return day_stats

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
                        type=int, required=True)
    parser.add_argument("-m", "--stats_month", help="Month you wish to get stats for",
                        type=int, required=True)
    parser.add_argument("-d", "--stats_day", help="Day you wish to get stats for",
                        type=int) 

    # For now, we require a particular station to be requested
    parser.add_argument("-i", "--station_id", 
                        help="SuperDARN Station ID you want stats for (e.g. 5)",
                        type=int, default=5, required=True)

    parser.add_argument("-v", "--verbose", help="Use verbose mode",
                        action="store_true")

    args = parser.parse_args()
    year = args.stats_year
    month = args.stats_month
    day = args.stats_day
    stid = args.station_id
    use_verbose = args.verbose
    return (year, month, day, stid, use_verbose)

def process_args(year, month, day, stid, use_verbose, cur):
    """
    Encapsulates the necessary logic to decide what to do based on 
    command-line arguments.
    """
    if use_verbose:
        print("Verbose!")
        initialize_logger(use_verbose)
    else:
        print("Not verbose!")
        initialize_logger(use_verbose)

    if day is not None:
        stats = stats_day(year, month, day, cur, stid=stid) 
    else:
        stats = stats_month(year, month, cur, stid=stid)
    return stats

def initialize_logger(use_verbose):
    """
    Function for setting up the initial logging parameters

    :param use_verbose: [boolean] flag indicating whether to be verbose
    """
    level = logging.DEBUG if use_verbose else logging.WARNING

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
    conn = rut.connect_db()
    cur = conn.cursor()
    year, month, day, stid, use_verbose = get_args()
    initialize_logger(use_verbose)        
    rut.read_config() 
    stats = process_args(year, month, day, stid, use_verbose, cur)
    print stats

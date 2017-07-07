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
import rawacf_utils as rut
from rawacf_utils import two_pad

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
    
    uptime_pct = sum(uptime_on_day.values())/SEC_IN_DAY
    return uptime_pct

def stats_month(year, month, cur, stid=5):
    """
    Calculates uptime stats for the entire month 
    """
    import calendar    
    last_day = calendar.monthrange(year, month)[1]
    days = map(rut.two_pad, np.arange(1, last_day + 1))
    day_stats = []
    for d in days:
        date_str = str(year) + rut.two_pad(month) + d
        day_stats.append(stats_day(date_str, cur, stid)) 
    return day_stats

if __name__ == "__main__":
    rut.read_config() 
    conn = rut.connect_db()
    cur = conn.cursor()
    cur.execute('select * from exps')
    tup = cur.fetchall()[0]

    print("Now creating RawacfRecord files")
    # Test RawacfRecord's class method constructors
    r2 = rut.RawacfRecord.record_from_tuple(tup)
    t = rut.get_tod_seconds(r2.start_dt)



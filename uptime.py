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
import backscatter 
import logging
import os
import sys

import sqlite3
import numpy as np

import dateutil.parser
from datetime import datetime as dt

logging.basicConfig(level=logging.INFO)

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
    import rawacf_metadata as rdat
    
    conn = rdat.connect_db()
    cur = conn.cursor()
    cur.execute('select * from exps')
    tup = cur.fetchall()[0]

    print("Now creating RawacfRecord files")
    # Test RawacfRecord's class method constructors
    r2 = rdat.RawacfRecord.record_from_tuple(tup)
    t = rdat.get_tod_seconds(r2.start_dt)


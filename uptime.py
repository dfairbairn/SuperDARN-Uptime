"""
file: 'uptime.py'
description:
    This script is to be used to calculate uptimes and downtimes for 
    SuperDARN arrays by analyzing the metadata entries in the 
    superdarntimes.sqlite database.

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
def stats_day(date_str, cur):
    """
    
    """
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
    return

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


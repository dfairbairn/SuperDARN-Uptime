"""
file: 'rawacf_utils.py'
description:
    This file (currently) contains all the methods and objects used 
    for parsing rawacf files, storing the experiment metadata in an 
    sql database, and performing high-level requests to process files
    in a given folder or to fetch and process specific dates of rawacf
    files. 

author: David Fairbairn
date: June 26 2017

"""
import backscatter 
import logging
import os
import sys
import subprocess

import sqlite3
import numpy as np

import dateutil.parser
from datetime import datetime as dt

logging.basicConfig(level=logging.DEBUG,
    format='%(levelname)s %(asctime)s: %(message)s', 
    datefmt='%m/%d/%Y %I:%M:%S %p')

class InconsistentRawacfError(Exception):
    """
    Raised when data from a rawacf file is inconsistent or incorrectly
    formatted, e.g. when a field which should be constant (e.g. origin 
    cmd) is inconsistent throughout a record.
    """

class RawacfRecord(object):
    """
    Class for containing a SuperDARN experiment record. Acquired by 
    parsing .rawacf files or from the sqlite database used in this 
    script.

    *** FIELDS ***
        - stid (station ID) : number corresponding to which array it is
        - start_dt: time of the start of the .rawacf entry (datetime obj)
        - end_dt : time of the end of the .rawacf entry (datetime obj)
        - cpid : Control program ID number
        - cmd_name : program name that was called to create this .rawacf 
                file (note: CPIDs and cmd_names should match eachother)
        - cmd_args : string containing the command-line args supplied when
                    running this command
        - nave_pos : boolean flag stating whether or not the 'n_ave'
                        parameter in the .rawacf file is consistently 
                        positive (if it were 0 or -, there'd be an issue)
        - times_consistent : boolean flag stating that the time difference
                        between entries in the .rawacf are small and 
                        consistent (if not, there was downtime during)
        - not_corrupt : boolean flag indicating if an exception occurred in 
                    parsing for or creating this record.
                * This is needed to indicate to uptime.parse_rawacf_folder()
                  when there's been non-critical exceptions raised in creating
                  a RawacfRecord instance (indicates if there were problems)*


    *** METHODS ***
        - Constructor (8 parameters)
        - __repr__ string
        - duration(): returns duration of this record in seconds
        - save_to_db(): saves the record as a database entry, given a db cursor
        - [Class method]: record_from_tuple(): build a RawacfRecord from a 
                tuple of relevant information (likely originating from database)
        - [Class method]: record_from_dics(): build a RawacfRecord from a 
                list of dict (dmap records) originating from a .rawacf file
    """
    def __init__(self, stid, start_dt, end_dt, cpid,
                 cmd_name, cmd_args, nave_pos, times_consistent, not_corrupt=True):
        """
        Self-explanatory constructor method.
        """
        self.start_dt = start_dt
        self.end_dt = end_dt
        self.stid = stid
        self.cpid = cpid
        self.cmd_name = cmd_name
        self.cmd_args = cmd_args 
        self.nave_pos = nave_pos
        self.times_consistent = times_consistent
        self.not_corrupt = not_corrupt

    def __repr__(self):
        """
        How to spit out this object's internals.
        """
        t0 = self.start_dt.isoformat()
        tf = self.end_dt.isoformat()
        cmd = self.cmd_name + " " + self.cmd_args
        string = "Record: from {0} to {1}\tCPID: {2}\n".format(t0, tf, self.cpid)
        string += "Origin Cmd: {0}\tNave status: {1}".format(cmd, self.nave_pos)
        string += "\tConsistent dT: {0}".format(self.times_consistent)
        return string

    def duration(self):
        """
        Computes the duration of the experiment in the record.
        
        :returns: total difference in seconds of end time minus start time
        """
        # datetimes can be subtracted to get intervals
        diff = self.end_dt - self.start_dt
        return diff.total_seconds()

    def save_to_db(self, cur):
        """
        Takes a cursor for an sqlite database and saves the object's 
        fields to the database

        :param cur: Cursor to an sqlite3 database to save to.
        """
        start_time = (self.start_dt).isoformat()
        end_time = (self.end_dt).isoformat()
        try:
            cur.execute('''INSERT INTO exps (stid, start_iso, end_iso, 
            cpid, cmd_name, cmd_args, nave_pos, times_consistent) 
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)''',
            (self.stid, start_time, end_time, 
            self.cpid, self.cmd_name, self.cmd_args, 
            int(self.nave_pos), int(self.times_consistent)))
        except sqlite3.IntegrityError:
            logging.error("Unique constraint failed or something.")     
        except sqlite3.OperationalError: 
            logging.error("\t\tDatabase locked - can't save metadata!")

    # Class method to read a tuple from the sqlite db and make a RawacfRecord
    @classmethod
    def record_from_tuple(cls, tup):
        """
        Creates and returns a RawacfRecord object constructed from the 
        contents of tuple 'tup' which is assumed to have been fetched 
        from the **SQLITE DB** (aka have the same order/structure as DB
        entries have, see the DB section).
    
        :param tup: [tuple] with 8 fields for making a RawacfRecord
                    (see RawacfRecord's constructor parameters)
        :returns: RawacfRecord object constructed from tuple's fields
            
        """
        assert(len(tup) == 8)
        assert(tup[0] != None and tup[1] != None and tup[2] != None)
        stid, start_iso, end_iso, cpid = (tup[:4])
        cmd_name, cmd_args, nave_pos, times_consistent = (tup[-4:])
        start_dt = iso_to_dt(start_iso)
        end_dt = iso_to_dt(end_iso)
        # Use contents of tuple as arguments for RawacfRecord constructor
        return cls(stid, start_dt, end_dt, cpid, cmd_name, cmd_args, 
                nave_pos, times_consistent)

    # Class method for making a RawacfRecord from a dicts from a .rawacf file
    @classmethod
    def record_from_dics(cls, dics):
        """
        Creates and returns a RawacfRecord object constructed from the 
        contents of a list of dictionaries that originates from parsing
        a **RAWACF FILE***

        :param dics: list of dicts from parsing a Rawacf into dmaps
        
        :returns: RawacfRecord constructed from information in the dicts
        """ 
        assert(type(dics)==list)
        assert(type(dics[0]==dict))
        assert(len(dics)>1) 
        # Parse the <theoretically> constant parameters for the experiment
        try:
            stid = process_field(dics, 'stid')
        except InconsistentRawacfError as e:
            logging.debug("\tInconsistency found in station ID: {0}".format(e))
            stid = -1
            not_corrupt = False
        try:
            cpid = process_field(dics, 'cp')
        except InconsistentRawacfError as e: 
            logging.debug("\tInconsistency found in cpid: {0}".format(e))
            cpid = -1
            not_corrupt = False
        try:
            cmd = process_field(dics, 'origin.command')
            cmd_spl =  cmd.split(' ',1)
            if len(cmd_spl)==1:
                cmd_name = cmd
                cmd_args = ""
            else:
                cmd_name = cmd.split(' ',1)[0]
                cmd_args = cmd.split(' ',1)[1]
        except InconsistentRawacfError:
            logging.debug("Inconsistency found in origin command")
            cmd_name = "<UnknownCommand>"
            cmd_args = ""
            not_corrupt = False
 
        # Parse the start/end temporal fields 
        start_dt = reconstruct_datetime(dics[0])
        end_dt = reconstruct_datetime(dics[-1])
        
        # Check for unusual N_ave values
        nave_pos = int(has_positive_nave(dics))
    
        # Check for downtime during the experiment's run
        ts = []
        for d in dics:
            ts.append(reconstruct_datetime(d))
        diffs = [(ts[i+1] - ts[i]).total_seconds() for i in range( len(ts) - 1 )]
        # logging.info(diffs)

        # Check that every difference between entries is 20 seconds or less
        times_consistent = int(( np.array(diffs) < 20 ).all())

        if 'not_corrupt' not in locals():
            not_corrupt = True
        return cls(stid, start_dt, end_dt, cpid, cmd_name, cmd_args, nave_pos, 
                   times_consistent, not_corrupt)

# -----------------------------------------------------------------------------
#                               Utility Methods 
# -----------------------------------------------------------------------------

      
def bz2_dic(fname):
    """ 
    Takes a compressed .rawacf file (in .bz2 format) and uses the
    backscatter library to retrieve a dictionary of a dmap object 
    parsed from its contents

    :param fname: path + filename of the rawacf.bz2 file
    
    :returns: list of dictionaries from backscatter lib's parsing of
                the .rawacf file
    """
    import bz2
    if not os.path.isfile(fname):
        logging.error('Not a file!')
        return None
    if fname[-4:] != '.bz2':
        logging.error('Not a .bz2 file!')
        return None
    f = bz2.BZ2File(fname,'rb')
    stream = f.read()
    dics = backscatter.dmap.parse_dmap_format_from_stream(stream)
    return dics

def acf_dic(fname):
    """ 
    Takes a .rawacf file and uses the backscatter library to retrieve 
    a dictionary of a dmap object parsed from its contents

    :param fname: path + filename of the .rawacf file

    :returns: list of dictionaries from backscatter lib's parsing of
                the .rawacf file
    """
    if not os.path.isfile(fname):
        logging.error('Not a file!')
        return None
    if fname[-7:] != '.rawacf':
        logging.error('Not a .rawacf file!')
        return None
    f = open(fname,'rb')
    stream = f.read()
    dics = backscatter.dmap.parse_dmap_format_from_stream(stream)
    return dics

def globus_connect():
    """
    Function for encapsulating the process of connecting to Globus.
    """
    _ = subprocess.check_output([GLOBUS_STARTUP_LOC, '-start', '&'])

def globus_query(script_query):
    """
    Function to encapsulate the process of making a Globus file request.

    :param script_query: [str] the query to hand Globus 
    """

    logging.info("Preparing to query: {0}".format(script_query))
    try:
        fetch = subprocess.check_output(script_query)
        logging.info("Fetch request answered with: {0}".format(fetch))
    except subprocess.CalledProcessError as e:
        logging.error("\t\tFailed Globus query. Exception given: {0}\n".format(e))
        logging.exception(e)
        return
    except OSError:
        logging.error("\t\tFailed to call Globus script")

def read_config(cfg_file='config.ini'):
    """
    Reads the local config file which provides definitions for a few global
    path variables for the script.

    [:param cfg_file: [str] stating the name of the config file to look for]
    """
    import configparser as cps
    try:
        f = open(cfg_file,'r')
    except IOError:
        logging.error("No config file found! Configure your script environment.")
        logging.info("Creating a sample configuration file...")
        with open('sample_config.ini','w') as f:
            f.write(
                "# Change this filename to 'config.ini' and make the path\n" +
                "# variables below point to the proper locations\n" +
                "[Paths]\n"
                "HOMEF: /path/to/homefolder\n" +
                "ENDPOINT: /path/to/globus_endpoint\n" +
                "GLOBUS_STARTUP_LOC: /path/to/globus_startupscript_folder\n" +
                "SYNC_SCRIPT_LOC: /path/to/kevins_globus_sync_script")
        return
    config = cps.ConfigParser()
    config.read_file(f)
   
    global HOMEF
    global ENDPOINT
    global GLOBUS_STARTUP_LOC
    global SYNC_SCRIPT_LOC 
    HOMEF = config.get('Paths','HOMEF')
    ENDPOINT = config.get('Paths','ENDPOINT')
    GLOBUS_STARTUP_LOC = config.get('Paths','GLOBUS_STARTUP_LOC')
    SYNC_SCRIPT_LOC = config.get('Paths','SYNC_SCRIPT_LOC')

def reconstruct_datetime(dic):
    """
    Takes a dictionary of a dmap and constructs a datetime object from 
    the dmap object's time fields

    :param dic: a single dictionary from DMAP, containing time info

    :returns: time information in a python [Datetime] object
    """
    t = dt(dic['time.yr'], dic['time.mo'], dic['time.dy'], dic['time.hr'], 
           dic['time.mt'], dic['time.sc'], dic['time.us']) 
    return t

def process_field(dics, field):
    """
    Takes a list of dictionaries representing the dmap object for a 
    rawacf file as well as a particular field, and extracts the field 
    and checks that the field has the same value for each dictionary in
    the list. If not, raises an exception indicating likely corrupted record

    :param dics: the list of dicts from backscatter lib's parse of a .rawacf
    
    :returns: [abstract] the value that's been requested if it's consistent    
    """
    val = dics[0][field]
    for i in dics:
        if i[field] != val:
            dbg_str = "process_field() was seeing record of {0}".format(val)
            dbg_str += " for '{0}' but now sees {1}".format(field, i[field])
            raise InconsistentRawacfError(dbg_str)
    return val

def has_positive_nave(dics):
    """
    Checks for correct values of 'nave' - the number of pulses detected.
    
    :param dics: the list of dicts from backscatter lib's parse of a .rawacf

    :returns: [boolean] True/False stating whether all vals of 'nave' are positive
    """
    for i in dics:
        if i['nave'] <= 0:
            return False
    return True 

def two_pad(num):
    """ 
    Takes in a number of 1 or 2 digits, returns a string of two digits. 

    :param num: an [int] between 0 and 99

    :returns: a [str] of length 2
    """
    assert isinstance(num,int)
    assert (num < 100 and num >= 0)
    return "0" + str(num) if str(num).__len__() == 1 else str(num)

def get_datestr(dt_obj):
    """
    Return a datestring in format "20160418". 

    :param dt_obj: a [Datetime] object
    
    :returns: a [str] of format yyyymmdd
    """
    return str(dt_obj.year) + two_pad(dt_obj.month) + two_pad(dt_obj.day)

def get_timestr(dt_obj):
    """
    Return a time in the format "01:00:00"

    :param dt_obj: a [Datetime] object

    :returns: a [str] of format hh:mm:ss (hours, min, sec)
    """
    return str(dt_obj.hour) + ":" + str(dt_obj.minute) + ":" + str(dt_obj.second)

def get_tod_seconds(dt_obj):
    """
    Returns the time of day (since 00h00m00s) in seconds

    :param dt_obj: a [Datetime] object

    :returns: a [float] of seconds since the start of the day '00:00:00'
    """
    return dt_obj.hour*3600. + dt_obj.minute*60. + dt_obj.second + 1E-6*dt_obj.microsecond

def iso_to_dt(iso):
    """
    Parses an iso formatted time, returns datetime object

    :param iso: a [str] of a date & time in ISO format 
                e.g. "2017-06-30T10:51:43.68922"

    :returns: a [Datetime] object
    """
    yr,mo,dy = map(int,(iso.split("T")[0]).split('-'))
    hr,mt,sc = (iso.split("T")[1]).split(':')
    hr, mt = map(int, [hr, mt])
    # In some exceptional cases there are no us, so handle this carefully
    if len(sc.split('.')) == 1:
        # Case where there's no microseconds
        logging.debug("No microseconds!")
        sc = int(sc)
        us = 0
    else:
        sc, us = map(int, sc.split('.'))
    out = dt(yr, mo, dy, hr, mt, sc, us)
    return out

def clear_endpoint():
    """
    Standalone function which will clear everything in the endpoint
    """
    if 'ENDPOINT' not in globals():
        read_config()
    for fil in os.listdir(ENDPOINT):
        try:
            subprocess.call(['rm',ENDPOINT+"/"+fil])
        except Exception as e:
            logging.error("Exception thrown during removal of file {0}".format(fil))
            logging.exception(e)

def month_year_iterator(start_month, start_year, end_month, end_year):
    """ Found on stackoverflow by user S.Lott.
    
    :param start_month: the month you wish to start your iterator on, integer
    :param start_year: the year you wish to start your iterator on, integer
    :param end_month: the month you wish to end your iterator on, integer
    :param end_year: the year you wish to end your iterator on, integer
    :returns: An iterator over a set of years and months
    """
    ym_start = 12 * start_year + start_month - 1
    ym_end = 12 * end_year + end_month - 1
    for ym in range(ym_start, ym_end):
        y, m = divmod(ym, 12)
        yield y, m + 1

# -----------------------------------------------------------------------------
#                              DB Methods 
# -----------------------------------------------------------------------------
def connect_db(dbname="superdarntimes.sqlite"):
    """
    Connects to a database for storing experiment metadata parsed from 
    rawacf files.

    Entries in the Experiments Table have the following fields:
    - stid (station ID) : 
    - start_iso : datetime of the start of the .rawacf entry (isoformat)
    - end_iso : datetime of the end of the .rawacf entry (isoformat)
    - cpid : Control program ID number
    - cmd_name : program name that was called to create this .rawacf 
                file (note: CPIDs and cmd_names should match eachother)
    - cmd_args : string containing the command-line args supplied when
                running this command
    - nave_pos : boolean flag stating whether or not the 'n_ave'
                    parameter in the .rawacf file is consistently 
                    positive (if it were 0 or -, there'd be an issue)
    - times_consistent : boolean flag stating that the time difference
                        between entries in the .rawacf are small and 
                        consistent (if not, there was downtime during)

    *** nave_pos and times_consistent are currently stored as integers
    expected to only take on values of "1" or "0" ***
    """
    #TODO:  maybe should check that it has the right structure?
    
    conn = sqlite3.connect(dbname)
    cur = conn.cursor()
    cur.executescript("""
    CREATE TABLE IF NOT EXISTS exps (
    stid integer NOT NULL,
    start_iso text NOT NULL,
    end_iso text NOT NULL,
    cpid integer NOT NULL,
    cmd_name text NOT NULL,
    cmd_args text,
    nave_pos BOOLEAN,
    times_consistent BOOLEAN,
    PRIMARY KEY (stid, start_iso)
    );
    """) 
    # And if it *did* exist, make sure it has all the necessary fields:
    cur.execute('PRAGMA table_info (exps)')
    flds = ['stid', 'start_iso', 'end_iso', 'cpid', 'cmd_name', 'cmd_args', 
            'nave_pos', 'times_consistent']
    tbl_flds = [ en[1] for en in cur.fetchall() ]
    for f in flds:
        if f not in tbl_flds:
            logging.error("Database incorrectly configured.")
            return None
    return conn

def clear_db(cur):
    """
    Clears all experiment information in the sqlite3 database.
    """
    cur.executescript("""
    DROP TABLE IF EXISTS exps;
    
    CREATE TABLE IF NOT EXISTS exps (
    stid integer NOT NULL,
    start_iso text NOT NULL,
    end_iso text NOT NULL,
    cpid integer NOT NULL,
    cmd_name text NOT NULL,
    cmd_args text,
    nave_pos BOOLEAN,
    times_consistent BOOLEAN,
    PRIMARY KEY (stid, start_iso)
    );
    """) 

def process_experiment(dics, conn):
    """
    Takes a dmap-based list of dicts 'dics' for a SuperDARN experiment
    and enters the key statistics for the experiment into the sqlite
    database pointed to by cursor 'cur'
    """
    cur = conn.cursor()
    r = RawacfRecord.record_from_dics(dics)
    return r

def select_exps(sql_select, cur):
    """
    Takes an sql query to select certain experiments, returns the list
    of RawacfRecord objects
    """
    logging.info("Querying with the following string:\n{0}".format(sql_select))
    cur.execute(sql_select)
    entries = cur.fetchall()
    records = []
    for entry in entries:
        # Do construction of experiment object from SQL output
        logging.debug("Looking at entry: {0}".format(entry))
        records.append(RawacfRecord.record_from_tuple(entry))
    return records

def dump_db(cur):
    """
    Shows all the entries in the DB
    """
    cur.execute('''select * from exps''')
    print cur.fetchall()

if __name__ == "__main__":
    read_config()         

    if len(sys.argv) > 1:
        path = sys.argv[1]
        if os.path.isdir(path):
            #clear_db() # whoa there buddy thats a bit rash
            conn = connect_db()
            cur = conn.cursor()



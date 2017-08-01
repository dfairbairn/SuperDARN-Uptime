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

CONSISTENT_RAWACF_THRESH = 20

radars16 = {'cly': 66, 'gbr': 1, 'han': 10, 'hok': 40, 'hkw': 41, 'inv': 64,
            'kap': 3, 'ksr': 16, 'kod': 7, 'lyr': 90, 'pyk': 9, 'pgr': 6, 
            'rkn': 65, 'sas': 5, 'sch': 2, 'sto': 8, 'dce': 96, 'fir': 21,
            'hal': 4, 'ker': 15, 'mcm': 20, 'san': 11, 'sps': 22, 'sye': 13,
            'sys': 12, 'tig': 14, 'unw': 18, 'zho': 19}
radars22 = {'ade': 209, 'adw': 208, 'fhe': 205, 'fhw': 204, 'bpk': 24}
radars24 = {'bks': 33, 'cve': 207, 'cvw': 206, 'wal': 32}
allradars = radars16.copy()
allradars.update(radars22)
allradars.update(radars24)

class InconsistentRawacfError(Exception):
    """
    Raised when data from a rawacf file is inconsistent or incorrectly
    formatted, e.g. when a field which should be constant (e.g. origin 
    cmd) is inconsistent throughout a record.
    """

class BadRawacfError(backscatter.dmap.DmapDataError):
    """
    An internal error type associated with a bad Rawacf file.
    This is currently only associated with finding a rawacf file with only
    one entry.
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
        - min_nave : the minimum value of n_ave in the .rawacf file
                    (entries of <= 0 indicate failure to retrieve data)
        - times_consistent : boolean flag stating that the time difference
                        between entries in the .rawacf are small and 
                        consistent (if not, there was downtime during)
        - not_corrupt : boolean flag indicating if an exception occurred in 
                    parsing for or creating this record.
                * This is needed to indicate to uptime.parse_rawacf_folder()
                  when there's been non-critical exceptions raised in creating
                  a RawacfRecord instance (indicates if there were problems)*
        - min_tfreq: lowest transmitting frequency used in this record
        - max_tfreq: highest transmitting frequency used in this record
        - xcf: ??

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
    def __init__(self, stid, start_dt, end_dt, cmd_name="", cmd_args="", cpid=0,
                 min_nave=0, times_consistent=True, not_corrupt=True,
                 min_tfreq=0., max_tfreq=0., xcf=0.):
        """
        Self-explanatory constructor method.
        """
        self.start_dt = start_dt
        self.end_dt = end_dt
        self.stid = stid
        self.cpid = cpid
        self.cmd_name = cmd_name
        self.cmd_args = cmd_args 
        self.min_nave = min_nave
        self.times_consistent = times_consistent
        self.not_corrupt = not_corrupt
        self.min_tfreq = min_tfreq
        self.max_tfreq = max_tfreq
        self.xcf = xcf

    def __repr__(self):
        """
        How to spit out this object's internals.
        """
        t0 = self.start_dt.isoformat()
        tf = self.end_dt.isoformat()
        cmd = self.cmd_name + " " + self.cmd_args
        string = "Record: from {0} to {1}\tCPID: {2}\n".format(t0, tf, self.cpid)
        string += "Origin Cmd: {0}\tNave status: {1}".format(cmd, self.min_nave)
        string += "\tConsistent dT: {0}".format(self.times_consistent)
        string += "\tTx freq min/max: {0}/{1}".format(self.min_tfreq, self.max_tfreq)
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
            cmd_name, cmd_args, cpid, min_nave, times_consistent, not_corrupt,
            min_tfreq, max_tfreq, xcf) 
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)''',
            (self.stid, start_time, end_time, 
            self.cmd_name, self.cmd_args, self.cpid,
            int(self.min_nave), int(self.times_consistent), 
            int(self.not_corrupt), self.min_tfreq, self.max_tfreq, self.xcf))
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
        assert(len(tup) == 12)
        assert(tup[0] != None and tup[1] != None and tup[2] != None)
        stid, start_iso, end_iso = (tup[:3])
        cmd_name, cmd_args, cpid = (tup[3:6])
        min_nave, times_consistent, not_corrupt = (tup[6:9])
        min_tfreq, max_tfreq, xcf = (tup[-3:])
        start_dt = iso_to_dt(start_iso)
        end_dt = iso_to_dt(end_iso)
        # Use contents of tuple as arguments for RawacfRecord constructor
        return cls(stid, start_dt, end_dt, cmd_name=cmd_name, cmd_args=cmd_args,
                cpid=cpid, min_nave=min_nave, times_consistent=times_consistent,
                not_corrupt=not_corrupt, min_tfreq=min_tfreq, 
                max_tfreq=max_tfreq, xcf=xcf)

    # Class method for making a RawacfRecord from a dicts from a .rawacf file
    @classmethod
    def record_from_dics(cls, dmap_dicts):
        """
        Creates and returns a RawacfRecord object constructed from the 
        contents of a list of dictionaries that originates from parsing
        a **RAWACF FILE***

        :param dmap_dicts: list of dicts from parsing a Rawacf into dmaps
        
        :returns: RawacfRecord constructed from information in the dicts
        """ 
        assert(type(dmap_dicts)==list)
        assert(type(dmap_dicts[0]==dict))
        if len(dmap_dicts) <= 1:
            logging.error("** Rare circumstance: A single-entry rawacf dmap! ***")
            err_str = "DMAP record found with only one data point. "
            raise BadRawacfError(err_str)

        objection_dict = check_fields(dmap_dicts)
        cpid = dmap_dicts[0]['cp'] if 'cp' not in objection_dict else -1
        stid = dmap_dicts[0]['stid'] if 'stid' not in objection_dict else -1
        cmd  = dmap_dicts[0]['origin.command'] if 'origin.command' not in objection_dict else "" 
        xcf = dmap_dicts[0]['xcf'] if 'xcf' not in objection_dict else -1
        cmd_spl =  cmd.split(' ',1)
        if len(cmd_spl)==1:
            cmd_name = cmd
            cmd_args = ""
        else:
            cmd_name = cmd_spl[0]
            cmd_args = cmd_spl[1]
        # If there were any 'inconsistency'-related objections, then we label the file as 'not_corrupt'=False
        for err_str in objection_dict.values():
            not_corrupt = False 
            logging.debug(err_str)

        # ** Grab tfreq **
        tfreqs = [ d['tfreq'] for d in dmap_dicts ]
        min_tfreq = min(tfreqs)
        max_tfreq = max(tfreqs)

        # ** Get the lowest n_ave value **
        min_nave = min([ d['nave'] for d in dmap_dicts ])
 
        # Parse the start/end temporal fields 
        try:
            start_dt = reconstruct_datetime(dmap_dicts[0])
            end_dt = reconstruct_datetime(dmap_dicts[-1])
             
            # Check for downtime during the experiment's run
            ts = []
            for d in dmap_dicts:
                ts.append(reconstruct_datetime(d))
        except ValueError:
            logging.error("Possible microsecond-related error.", exc_info=True)
            err_str = "Microseconds in start and end dts: {0}, {1}"
            logging.error(err_str.format(dmap_dicts[0]['time.us'], dmap_dicts[-1]['time.us']))
        diffs = [(ts[i+1] - ts[i]).total_seconds() for i in range( len(ts) - 1 )]
        # Check that every difference between entries is 20 seconds or less
        times_consistent = int(( np.array(diffs) < CONSISTENT_RAWACF_THRESH ).all())

        if 'not_corrupt' not in locals():
            not_corrupt = True
        return cls(stid, start_dt, end_dt, cmd_name=cmd_name, cmd_args=cmd_args,
                    cpid=cpid, min_nave=min_nave, times_consistent=times_consistent, 
                    not_corrupt=not_corrupt, min_tfreq=min_tfreq, 
                    max_tfreq=max_tfreq, xcf=xcf)

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
        raise IOError('Not a file! {0}'.format(fname))
    if fname[-4:] != '.bz2':
        raise IOError('Not a .bz2 file! {0}'.format(fname))
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
        raise IOError('Not a file!')
    if fname[-7:] != '.rawacf':
        raise IOError('Not a .rawacf file!')
    f = open(fname,'rb')
    stream = f.read()
    dics = backscatter.dmap.parse_dmap_format_from_stream(stream)
    return dics

def globus_connect():
    """
    Function for encapsulating the process of connecting to Globus.

    ** NOTE **
    Although it's generally expected that folks should use subprocess.call
    instead of os.system nowadays, I found that only os.system worked for this.

    > subprocess.Popen([str(rut.GLOBUS_STARTUP_LOC), '-start', '&'])
    > subprocess.Popen([str(rut.GLOBUS_STARTUP_LOC), '-start', '&'], 
                        stdout=subprocess.PIPE)

    The above two python lines only produce <defunct> processes that don't 
    function as globus endpoints

    > subprocess.Popen([str(rut.GLOBUS_STARTUP_LOC), '-start', '&'], 
                        stdout=subprocess.PIPE, shell=True)

    The above line returns the help prompt from the globusconnect script (as 
    if no args were given)
    """
    _ = os.system(GLOBUS_STARTUP_LOC + ' -start &')
    logging.info("Subprocess received message {0}".format(_) + \
            " from attempting globus startup.")

def globus_disconnect():
    """
    Kills the globus connection by searching active processes for the ones
    that have 'globusonline' in their command name/args.
    """
    procs = subprocess.Popen(['ps','-u'], stdout=subprocess.PIPE)
    grep = subprocess.Popen(['grep', 'globusonline'], 
                                    stdin=procs.stdout, stdout=subprocess.PIPE)
    cut = subprocess.check_output(['cut', '-d', ' ', '-f', '3'], stdin=grep.stdout)
    for pid in cut.split('\n')[:-1]:
        logging.debug("Preparing to kill PID #{0}...".format(pid))
        try:
            out = subprocess.check_output(['kill','-s','SIGKILL',pid])
        except subprocess.CalledProcessError as e:
            logging.debug("Error with kill of {0}: {1}".format(pid, e))

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
                "SYNC_SCRIPT_LOC: /path/to/kevins_globus_sync_script\n" +
                "# Include the actual filename of the 'globusconnect' and " +
                "# 'sync_radar_data_globus.py' files.")
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
    # There are a couple spurious cases of 0 or negative microseconds that 
    # mess things up, so here I catch them and set them to 1us
    if dic['time.us'] < 0 or dic['time.us'] > 999999 or type(dic['time.us']) != int:
        err_str = "Microseconds value is : {0}".format(dic['time.us'])
        err_str += "\t Setting it to 1 us before proceeding..."
        logging.warning(err_str)
        dic['time.us'] = 1
    t = dt(dic['time.yr'], dic['time.mo'], dic['time.dy'], dic['time.hr'], 
           dic['time.mt'], dic['time.sc'], dic['time.us']) 
    return t

def check_fields(dmap_dicts):
    """
    Takes a list of dictionaries representing the dmap object for a 
    rawacf file as well as a particular field, and extracts the field 
    and checks that the field has the same value for each dictionary in
    the list. If not, raises an exception indicating likely corrupted record

    :param dics: the list of dicts from backscatter lib's parse of a .rawacf
    
    :returns: [abstract] the value that's been requested if it's consistent    
    """
    objection_dict = dict()
    for i, dmap_dict in enumerate(dmap_dicts):
        # Check if some fields are consistent throughout
        for field in ['cp', 'origin.command', 'stid', 'xcf']: 
            val = dmap_dict[field]
            first_val = dmap_dicts[0][field]
            if first_val != val:
                # Current value of field is different from first value
                dbg_str = "\t\tcheck_field() was seeing record of {0} for ".format(first_val)
                dbg_str += "'{0}' but now sees {1} at index {2} of {3}".format(field, val, i, len(dmap_dicts))
                objection_dict[field] = dbg_str
                #raise InconsistentRawacfError(dbg_str)
        # Check if rsep corresponds to txpl
        txpl = dmap_dict['txpl']
        rsep = dmap_dict['rsep']
        if (txpl*3/20) != rsep:
            dbg_str = "Fields 'rsep' and 'txpl' are inconsistent with each other."
            dbg_str += "\trsep: {0}, txpl: {1}".format(rsep, txpl)
            objection_dict['rsep'] = objection_dict['txpl'] = dbg_str
        # Check if bmnum is valid ?
        range_max = 16 if dmap_dict['stid'] in radars16.values() else 24
        if dmap_dict['bmnum'] not in range(range_max):
            dbg_str = "Saw unexpected value of 'bmnum'"
            objection_dict['bmnum'] = dbg_str
    return objection_dict

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
#    return str(dt_obj.year) + two_pad(dt_obj.month) + two_pad(dt_obj.day)
    return str(dt_obj.year) + "{:02d}".format(dt_obj.month) + "{:02d}".format(dt_obj.day)

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
    - min_nave : the smallest value of the 'n_ave' parameter
                    (if it's were 0 or -, there's be an issue)
    - times_consistent : boolean flag stating that the time difference
                        between entries in the .rawacf are small and 
                        consistent (if not, there was downtime during)
    - not_corrupt : flag associated with records that are identified to have
                    unexpected or possibly corrupted data
    - min_tfreq:
    - max_tfreq:
    - xcf:


    *** not_corrupt and times_consistent are currently stored as integers
    expected to only take on values of "1" or "0" ***
    """
    
    conn = sqlite3.connect(dbname)
    cur = conn.cursor()
    cur.executescript("""
    CREATE TABLE IF NOT EXISTS exps (
    stid integer NOT NULL,
    start_iso text NOT NULL,
    end_iso text NOT NULL,
    cmd_name text,
    cmd_args text,
    cpid integer,
    min_nave integer,
    times_consistent BOOLEAN,
    not_corrupt BOOLEAN,
    min_tfreq integer,
    max_tfreq integer,
    xcf integer,
    PRIMARY KEY (stid, start_iso)
    );
    """) 
    db_correct = check_db(cur)
    if not db_correct:
        logging.error("Database incorrectly configured.")
    return conn

def check_db(cur):
    """
    Given a cursor to a DB, checks that it has the right structuring.
    """
    # And if it *did* exist, make sure it has all the necessary fields:
    cur.execute('PRAGMA table_info (exps)')
    flds = ['stid', 'start_iso', 'end_iso', 'cmd_name', 'cmd_args', 'cpid', 
            'min_nave', 'times_consistent', 'not_corrupt', 'min_tfreq',
            'max_tfreq', 'xcf']
    tbl_flds = [ en[1] for en in cur.fetchall() ]
    db_correct = True
    for f in flds:
        if f not in tbl_flds:
            db_correct = False
    return db_correct
    
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
    cmd_name text,
    cmd_args text,
    cpid integer,
    min_nave integer,
    times_consistent BOOLEAN,
    not_corrupt BOOLEAN,
    min_tfreq integer,
    max_tfreq integer,
    xcf integer,
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
    r.save_to_db()
    return r

def select_exps(sql_select, cur):
    """
    Takes an sql query to select certain experiments, returns the list
    of RawacfRecord objects
    """
    logging.debug("Querying with the following string:\n{0}".format(sql_select))
    cur.execute(sql_select)
    entries = cur.fetchall()
    records = []
    for entry in entries:
        # Do construction of experiment object from SQL output
        logging.debug("Looking at entry: {0}".format(entry))
        records.append(RawacfRecord.record_from_tuple(entry))
    return records

def dump_db(conn):
    """
    Shows all the entries in the DB
    """
    cur = conn.cursor()
    cur.execute('delete from exps')
    conn.commit()

if __name__ == "__main__":
    read_config()         

    if len(sys.argv) > 1:
        path = sys.argv[1]
        if os.path.isdir(path):
            #clear_db() # whoa there buddy thats a bit rash
            conn = connect_db()
            cur = conn.cursor()

    conn = connect_db()
    cur = conn.cursor()
    

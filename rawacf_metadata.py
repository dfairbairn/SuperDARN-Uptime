"""
file: 'rawacf_metadata.py'
description:
    This file contains the methods and objects used for parsing rawacf
    files and storing the experiment metadata in an sql database. 

author: David Fairbairn
date: June 26 2017

"""
import backscatter 
import logging
import os
import sys

import sqlite3
import numpy as np

import dateutil.parser
from datetime import datetime as dt

logging.basicConfig(level=logging.INFO, 
    format='%(levelname)s %(asctime)s: %(message)s', 
    datefmt='%m/%d/%Y %I:%M:%S %p')

HOMEF = "/home/david"
ENDPOINT = HOMEF + "/globus/tst-endpoint"
GLOBUS_STARTUP_LOC = HOMEF + "/globus/globusconnectpersonal-2.3.3/globusconnect"
SYNC_SCRIPT_LOC = HOMEF + "/globus/sync_radar_data_globus.py"

class BadRawacfDataError(Exception):
    """
    Raised when data from a rawacf file is inconsistent or incorrectly
    formatted, e.g. when a field which should be constant (e.g. origin 
    cmd) is inconsistent throughout a record.
    """
    pass

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


    """
    def __init__(self, stid, start_dt, end_dt, cpid,
                 cmd_name, cmd_args, nave_pos, times_consistent):
        self.start_dt = start_dt
        self.end_dt = end_dt
        self.stid = stid
        self.cpid = cpid
        self.cmd_name = cmd_name
        self.cmd_args = cmd_args 
        self.nave_pos = nave_pos
        self.times_consistent = times_consistent

    def __repr__(self):
        """
        How to spit out this thing's internals.
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
        """
        # datetimes can be subtracted to get intervals
        diff = end_dt - start_dt
        return diff.total_seconds()

    def save_to_db(self, cur):
        """
        Takes a cursor for an sqlite database and saves the object's 
        fields to the database
        """
        start_time = (self.start_dt).isoformat()
        end_time = (self.end_dt).isoformat()
        cur.execute('''INSERT INTO exps (stid, start_iso, end_iso, 
        cpid, cmd_name, cmd_args, nave_pos, times_consistent) 
        VALUES (?, ?, ?, ?, ?, ?, ?, ?)''',
        (self.stid, start_time, end_time, 
        self.cpid, self.cmd_name, self.cmd_args, 
        int(self.nave_pos), int(self.times_consistent)))

    # Class method to read a tuple from the sqlite db and make a RawacfRecord
    @classmethod
    def record_from_tuple(cls, tup):
        """
        Creates and returns a RawacfRecord object constructed from the 
        contents of tuple 'tup' which is assumed to have been fetched 
        from the **SQLITE DB** (aka have the same order/structure as DB
        entries have, see the DB section).
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
        """ 
        assert(type(dics)==list)
        assert(type(dics[0]==dict))
        assert(len(dics)>1) 
        # Parse the <theoretically> constant parameters for the experiment
        stid = process_field(dics, 'stid')
        cpid = process_field(dics, 'cp')
        cmd = process_field(dics, 'origin.command')
        cmd_name = cmd.split(' ',1)[0]
        cmd_args = cmd.split(' ',1)[1]
    
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
        return cls(stid, start_dt, end_dt, cpid, cmd_name, cmd_args, nave_pos, 
                   times_consistent)

# -----------------------------------------------------------------------------
#                               Utility Methods 
# -----------------------------------------------------------------------------

      
def bz2_dic(fname):
    """ 
    Takes a compressed .rawacf file (in .bz2 format) and uses the
    backscatter library to retrieve a dictionary of a dmap object 
    parsed from its contents
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

def reconstruct_datetime(dic):
    """
    Takes a dictionary of a dmap and constructs a datetime object from 
    the dmap object's time fields
    """
    t = dt(dic['time.yr'], dic['time.mo'], dic['time.dy'], dic['time.hr'], 
           dic['time.mt'], dic['time.sc'], dic['time.us']) 
    return t

def process_field(dics,field):
    """
    Takes a list of dictionaries representing the dmap object for a 
    rawacf file as well as a particular field, and extracts the field 
    and checks that the field has the same value for each dictionary in
    the list.
    """
    val = dics[0][field]
    for i in dics:
        if i[field] != val:
            raise BadRawacfDataError
    return val

def has_positive_nave(dics):
    """
    Checks for correct values of 'nave' - the number of pulses detected.
    """
    for i in dics:
        if i['nave'] <= 0:
            return False
    return True 

def two_pad(num):
    """ 
    Takes in a number of 1 or 2 digits, returns a string of two digits. 
    """
    assert isinstance(num,int)
    assert (num < 100 and num >= 0)
    return "0" + str(num) if str(num).__len__() == 1 else str(num)

def get_datestr(dt_obj):
    """
    Return a datestring in format "20160418". 
    """
    return str(dt_obj.year) + str(dt_obj.month) + str(dt_obj.day)

def get_timestr(dt_obj):
    """
    Return a time in the format "01:00:00"
    """
    return str(dt_obj.hour) + ":" + str(dt_obj.minute) + ":" + str(dt_obj.second)

def get_tod_seconds(dt_obj):
    """
    Returns the time of day (since 00h00m00s) in seconds
    """
    return dt_obj.hour*3600. + dt_obj.minute*60. + dt_obj.second + 1E-6*dt_obj.microsecond

def iso_to_dt(iso):
    """
    Parses an iso formatted time, returns datetime object
    """
    yr,mo,dy = map(int,(iso.split("T")[0]).split('-'))
    hr,mt,sc = (iso.split("T")[1]).split(':')
    hr, mt = map(int, [hr, mt])
    sc, us = map(int, sc.split('.'))
    out = dt(yr, mo, dy, hr, mt, sc, us)
    return out

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

def start_db(dbname="superdarntimes.sqlite"):
    """
    (Re)creates a database for storing experiment metadata parsed from 
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

    *** nave_pos and times_consistent are currently stored as strings
    expected to only take on values of "True" or "False" ***
    """
    logging.info("Restarting the sqlite db...")
    conn = sqlite3.connect(dbname)
    cur = conn.cursor()
 
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
    return conn

def connect_db(dbname="superdarntimes.sqlite"):
    """
    Connects to the database without messing around with it.
    """
    #TODO: But maybe checking that it has the right structure?
    conn = sqlite3.connect(dbname)
    return conn

def process_experiment(dics, cur):
    """
    Takes a dmap-based list of dicts 'dics' for a SuperDARN experiment
    and enters the key statistics for the experiment into the sqlite
    database pointed to by cursor 'cur'
    """
    r = RawacfRecord.record_from_dics(dics)
    r.save_to_db(cur)
    return r

def select_exps(sql_select):
    """
    Takes an sql query to select certain experiments, returns the list
    of RawacfRecord objects
    """
    cur.execute(sql_select)
    entries = cur.fetchall()
    records = []
    for entry in entries:
        # Do construction of experiment object from SQL output
        records.append(RawacfRecord.record_from_tuple(entry))
    return records

def dump_db(cur):
    """
    Shows all the entries in the DB
    """
    cur.execute('''select * from exps''')
    print cur.fetchall()

# -----------------------------------------------------------------------------
#                           High-Level Methods 
# -----------------------------------------------------------------------------

def process_rawacfs_dates(start_month, start_year, end_month, end_year):
    """
    The giant script that could be run which would repeatedly call 
    process_rawacfs_month (might do this using a bash script though)
    """
    import subprocess
    import calendar 

    start = str(start_year) + two_pad(start_month)
    end = str(end_year) + two_pad(end_month)
    logname = 'process_rawacf_{0}_{1}.log'.format(start, end)
    logging.basicConfig(filename=logname, level=logging.INFO)

    # TODO: MAKE THIS PROCESS USER/MACHINE AGNOSTIC
    # I. Run the globus connect process
    _ = subprocess.check_output([GLOBUS_STARTUP_LOC, '-start', '&'])

    # II. For each yr, month:
    for yr, mo in month_year_iterator(start_month, start_year, end_month, end_year):
        process_rawacfs_month(yr, mo)
 
def process_rawacfs_month(year, month):
    """
    Takes starting month and year and ending month and year as arguments. Steps
    through each day in each year/month combo
    """
    import subprocess
    import calendar 

    date = str(year) + two_pad(month)
    logname = 'process_rawacf_{0}.log'.format(date)
    logging.basicConfig(filename=logname, level=logging.INFO)

    # I. Run the globus connect process
    _ = subprocess.check_output([GLOBUS_STARTUP_LOC, '-start', '&'])

    logging.info("Beginning to process Rawacf logs... ")
    
    last_day = calendar.monthrange(yr, mo)[1]
    logging.info("Starting to analyze {0}-{1} files...".format(str(yr), two_pad(mo))) 

    # III. For each day in the month:
    for dy in np.arange(1,last_day):
        logging.info("\tLooking at {0}-{1}-{2}".format(
                     str(yr), two_pad(mo), two_pad(dy)))
        # A. First, grab the rawacfs via globus (and wait on it)
        script_query = [SYNC_SCRIPT_LOC,'-y', str(yr), '-m',
            str(mo), '-p', str(yr)+two_pad(mo)+two_pad(dy)+"*", ENDPOINT]
        logging.info("\t\tPreparing to query: {0}".format(script_query))
        try:
            fetch = subprocess.check_output(script_query)
            logging.info("\t\tFetch request answered with: {0}".format(fetch))
        except CalledProcessError:
            logging.error("\t\tFailed Globus query.")
        except OSError:
            logging.error("\t\tFailed to call Globus script")

        try:
            # B. Parse the rawacf files, save their metadata in our DB
            parse_rawacf_folder(ENDPOINT)
            logging.info("\t\tDone with parsing {0}-{1}-{2} rawacf data".format(
                         str(yr), two_pad(mo), two_pad(dy)))
        except OperationalError:
            logging.error("\t\tDatabase locked - can't save metadata!")

        # C. Clear the rawacf files that were fetched in this cycle
        try:
            subprocess.check_output(['rm','-rf', ENDPOINT])
            logging.info("\t\tDone with clearing {0}-{1}-{2} rawacf data".format(
                     str(yr), two_pad(mo), two_pad(dy)))
        except CalledProcessError:
            logging.error("\t\tUnable to remove files.")

def test_process_rawacfs():
    """
    This method exists specifically to test whether everything's 
    configured properly to run the script to grab an entire month or 
    year's data. It attempts to start globus
    """
    import subprocess
    # Test 1: running globusconnect 
    _ = subprocess.check_output([GLOBUS_STARTUP_LOC, '-start', '&'])
    #   If startup unsuccessful, check if its bc its already running

    # Test 2: perform a globus fetch using the script
    script_query = [SYNC_SCRIPT_LOC,'-y', '2017', '-m',
        '01', '-p', '20170101.2*sas', ENDPOINT]
    logging.info("Preparing to query: {0}".format(script_query))
    try:
        fetch = subprocess.check_output(script_query)
        logging.info("Fetch request answered with: {0}".format(fetch))
    except CalledProcessError:
        logging.error("\t\tFailed Globus query.")
    except OSError:
        logging.error("\t\tFailed to call Globus script")


    # Test 3: verify that we can parse this stuff
    try:
        parse_rawacf_folder(ENDPOINT)
        logging.info("Done with parsing 2017-01-01 'sas' rawacf data")
    except OperationalError:
        logging.error("\t\tDatabase locked - can't save metadata!")

    # Test 4: Clear the rawacf files that we fetched
    try:
        subprocess.check_output(['rm','-rf', ENDPOINT])
        logging.info("Successfully removed 2017-01-01 'sas' rawacf data")

    except CalledProcessError:
        logging.error("\t\tUnable to remove files")
 
def parse_rawacf_folder(folder):
    """
    Takes a path to a folder which contains of .rawacf files, parses them
    and inserts them into the database.
    """
    # For now just leave this empty; the stuff in if-main will go here
    assert(os.path.isdir(folder))
    logging.info("Acceptable path {0}. Analysis proceeding...".format(folder))
    conn = connect_db()
    cur = conn.cursor()
    for fil in os.listdir(path):
        logging.info("File {0}:".format(fil)) 
        if fil[-4:] == '.bz2':
            dics = bz2_dic(path+'/'+fil)
        elif fil[-7:] == '.rawacf':
            dics = acf_dic(path+'/'+fil)
        else:
            # Could do something with these too?
            logging.info('File not used for dmap records.')
            continue
        # If it was a bz2 or rawacf, now we do scripty stuff with dict
        process_experiment(dics, cur)
        # Commit the database changes
        conn.commit()

if __name__ == "__main__":

    # June 26: short term TODO: make it so that I don't do start_db() more than once
         

    if len(sys.argv) > 1:
        path = sys.argv[1]
        if os.path.isdir(path):
            # parse_rawacf_folder(sys.argv[1])
            logging.info("Acceptable path. Analysis proceeding...")
            conn = start_db()
            cur = conn.cursor()
            for fil in os.listdir(path):
                logging.info("File {0}:".format(fil)) 
                if fil[-4:] == '.bz2':
                    dics = bz2_dic(path+'/'+fil)
                elif fil[-7:] == '.rawacf':
                    dics = acf_dic(path+'/'+fil)
                else:
                    # Could do something with these too?
                    logging.info('File not used for dmap records.')
                    continue
                # If it was a bz2 or rawacf, now we do scripty stuff with dict
                process_experiment(dics, cur)
                # Now commit the changes
                conn.commit()


    # Tests and sample usage
    cur.execute('select * from exps')
    tup = cur.fetchall()[0]

    print("Now creating RawacfRecord files")
    # Test RawacfRecord's class method constructors
    r1 = RawacfRecord.record_from_dics(dics)
    r2 = RawacfRecord.record_from_tuple(tup)
    
    r2.stid = 90
    t = r2.start_dt
    r2.start_dt = dt(t.year, t.month, t.day, t.hour+1, t.minute, t.second)
    # Test RawacfRecord's save_to_db()
    r2.save_to_db(cur)
    # Test __repr__()
    print(r2)

    dump_db(cur)


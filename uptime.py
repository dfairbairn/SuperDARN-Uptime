"""
file: 'uptime.py'
description:
    This script is to be used to calculate uptimes and downtimes for 
    SuperDARN arrays based on dmap data gleaned from .rawacf files.

author: David Fairbairn
date: June 20 2017

"""

import backscatter
import logging
import os
import sys

import sqlite3
import numpy as np

from datetime import datetime as dt

logging.basicConfig(level=logging.INFO)

# -----------------------------------------------------------------------------
#                           UPTIME SCRIPT FUNCTIONS
# -----------------------------------------------------------------------------

class InconsistentDmapFieldError(Exception):
    """
    Raised when a field which should be constant (e.g. origin cmd) is
    inconsistent throughout a rawacf record.
    """
    pass

def parse_rawacfs():
    """
    Takes the command-line argument provided to this program and if
    valid, treats it as a path to a trove of .rawacf files, parses them
    and inserts them into a database.
    """
    # If we've been given 
    if len(sys.argv) > 1:
        path = sys.argv[1]
        if os.path.isdir(path):
            logging.info("Acceptable path. Analysis proceeding...")
            cur = start_db()
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

    dump_db(cur)   

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
            raise InconsistentDmapFieldError 
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


# -----------------------------------------------------------------------------
#                               DB FUNCTIONS
# -----------------------------------------------------------------------------

def start_db(dbname="superdarntimes.sqlite"):
    """
    (Re)creates a database for storing experiment metadata parsed from 
    rawacf files.

    Entries in the Experiments Table have the following fields:
    - stid (station ID) : 
    - start_date : date of the start of the .rawacf entry
    - start_time : time of day of the start of the .rawacf entry
    - end_date : date of the end of the .rawacf entry
    - end_time : time of day of the end of the .rawacf entry
    - cpid : Control program ID number
    - cmd_name : program name that was called to create this .rawacf 
                file (note: CPIDs and cmd_names should match eachother)
    - cmd_args : string containing the command-line args supplied when
                running this command
    - nave_positive : boolean flag stating whether or not the 'n_ave'
                    parameter in the .rawacf file is consistently 
                    positive (if it were 0 or -, there'd be an issue)
    - times_consistent : boolean flag stating that the time difference
                        between entries in the .rawacf are small and 
                        consistent (if not, there was downtime during)

    *** nave_positive and times_consistent are currently stored as strings
    expected to only take on values of "True" or "False" ***
    """
    logging.info("Starting up the sqlite db...")
    conn = sqlite3.connect(dbname)
    cur = conn.cursor()
    
    cur.executescript("""
    DROP TABLE IF EXISTS exps;
    
    CREATE TABLE IF NOT EXISTS exps (
    stid integer NOT NULL,
    start_date text NOT NULL,
    start_time text NOT NULL, 
    end_date text NOT NULL,
    end_time text NOT NULL,
    cpid integer NOT NULL,
    cmd_name text NOT NULL,
    cmd_args text,
    nave_positive BOOLEAN,
    times_consistent BOOLEAN,
    PRIMARY KEY (stid, start_date, start_time)
    );
    """) 
    return cur

def process_experiment(dics, cur):
    """
    Takes a dmap-based list of dicts 'dics' for a SuperDARN experiment
    and enters the key statistics for the experiment into the sqlite
    database pointed to by cursor 'cur'
    """
    # Parse the <theoretically> constant parameters for the experiment
    stid = process_field(dics, 'stid')
    cpid = process_field(dics, 'cp')
    cmd = process_field(dics, 'origin.command')
    cmd_name = cmd.split(' ',1)[0]
    cmd_args = cmd.split(' ',1)[1]

    # Parse the start/end temporal fields 
    t0 = reconstruct_datetime(dics[0])
    start_date = str(t0.year) + two_pad(t0.month) + two_pad(t0.day) 
    start_time = two_pad(t0.hour) + "h" + two_pad(t0.minute) + "m" + two_pad(t0.second) + "s"
    tf = reconstruct_datetime(dics[-1])
    end_date = str(tf.year) + two_pad(tf.month) + two_pad(tf.day) 
    end_time = two_pad(tf.hour) + "h" + two_pad(tf.minute) + "m" + two_pad(tf.second) + "s"

    # Check for unusual N_ave values
    nave_positive = has_positive_nave(dics)
    print nave_positive 

    # Check for downtime during the experiment's run
    ts = []
    for d in dics:
        ts.append(reconstruct_datetime(d))
    diffs = [(ts[i+1] - ts[i]).seconds for i in range( len(ts) - 1 )]
    # Check that every difference between entries is 20 seconds or less
    times_consistent = ( np.array(diffs) < 20 ).all() 

    logging.info("Record: from {0} to {1}\tCPID: {2}".format(t0, tf, cpid))
    logging.info("Origin Cmd: {1}\tNave status: {2}\tConsistent dT: {3}".format(cpid, cmd, 
                    nave_positive,times_consistent))

    # Perform the SQL insertion
    cur.execute('''INSERT INTO exps (stid, start_date, start_time, end_date, 
                end_time, cpid, cmd_name, cmd_args, nave_positive, 
                times_consistent)  VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)''',
                (stid, start_date, start_time, end_date, end_time, cpid, 
                cmd_name, cmd_args, int(nave_positive), int(times_consistent)))
    print("Entered that shit!")

def select_exps(sql_select):
    """
    Takes an sql query to select certain experiments, returns the list
    of experiment objects
    """
    cur.execute(sql_select)
    entries = cur.fetchall()
    for entry in entries:
        # Do construction of experiment object from SQL output
        pass 
    return

def extract_dict(exp_entry):
    """

    """
    # Take the tuple that comes from an sql fetch, store it in Experiment obj
    return

def dump_db(cur):
    """
    Shows all the entries in the DB
    """
    cur.execute('''select * from exps''')
    print cur.fetchall()

if __name__ == "__main__":
    parse_rawacfs()

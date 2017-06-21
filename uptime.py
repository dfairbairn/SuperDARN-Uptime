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
from datetime import datetime as dt

logging.basicConfig(level=logging.INFO)

class InconsistentDmapFieldError(Exception):
    """
    Raised when a field which should be constant (e.g. origin cmd) is
    inconsistent throughout a rawacf record.
    """
    pass

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

if __name__ == "__main__":
    if len(sys.argv) > 1:
        path = sys.argv[1]
        if os.path.isdir(path):
            logging.info("Acceptable path. Analysis proceeding...")
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
                t0 = reconstruct_datetime(dics[0])
                tf = reconstruct_datetime(dics[-1])
                cmd = process_field(dics, 'origin.command')
                cpid = process_field(dics, 'cp')
                nave_good = has_positive_nave(dics)
                logging.info("Record results: up from {0} to {1}".format(t0, tf))
                logging.info("CPID: {0}\t Origin Command: {1}\t Nave status: {2}".format(cpid, cmd, nave_good))


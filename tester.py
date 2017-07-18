"""
file: 'tester.py'
description:
    The long-awaited test file is here!

date: July 10th 2017
author: David Fairbairn

*NOTE*: To legitimately test process_rawcfs_day and process_rawacfs_month 
    requires actually calling them and performing the entire 10 GB or 250 GB
    of fetching respectively.

"""
import subprocess
import os

import rawacf_utils as rut
import parse
import uptime
import sqlite3
import multiprocessing as mp

UPTIME_ROOT_DIR = '.'
TESTS_DIR = UPTIME_ROOT_DIR + '/tests'
#UPTIME_ROOT_DIR='..'

TEST_BZFILE1 = 'tests/sample_files/20161201.0401.00.bks.rawacf.bz2'
TEST_BZFILE2 = 'tests/sample_files/20161201.1441.54.han.rawacf.bz2'
TEST_RAWACF = 'tests/sample_files/20170601.0001.00.sas.rawacf'


test_dmap_dicts = [{'cp': 3, 'origin.command': 'test', 'stid': 5, 'txpl': 300,
                    'rsep': 45, 'bmnum': 0},
                    {'cp': 3, 'origin.command': 'test', 'stid': 5, 'txpl': 300,
                    'rsep': 45, 'bmnum': 0},
                    {'cp': 3, 'origin.command': 'test', 'stid': 5, 'txpl': 300,
                    'rsep': 45, 'bmnum': 0}]

# ------------------------------------------------------------------------------
#                       Parse.py Tests: High-Level Methods
# ------------------------------------------------------------------------------

def test_process_month():
    """
    Tests the fetching and processing of a whole month's data in the endpoint
    in the tests directory.

    """
    #parse.process_rawacfs_month(2016, 12)
    subprocess.call(['./parse.py', '-y', '2016', '-m', '12'])
    return None 

def test_process_day():
    """
    Tests fetching and processing of a whole day's data in the endpoint in the
    tests directory    
    """
    #parse.process_rawacfs_month(2016, 11, 30)
    subprocess.call(['./parse.py', '-y', '2016', '-m', '11', '-d', '30'])
    subprocess.call(['./parse.py', '-y', '2016', '-m', '11', '-d', '29', '-c', 'sas'])
    return None

def test_parse_folder():
    """
    Essentially tests parse_rawacf_folder() on a significant amount of 
    pre-existing data.
    
    """
    subprocess.call(['./parse.py', '-p', '{0}/acf/endpoint2'.format(UPTIME_ROOT_DIR)])
    #parse.parse_rawacf_folder('acf/endpoint2')    
    return None

def test_process_file():
    """
    Tests process_file() on a particular pre-existing rawacf data file.
    """
    #parse.process_file('{0}/acf/endpoint2/20170101.0000.01.fhe.rawacf.bz2'.format(UPTIME_ROOT_DIR))    
    subprocess.call(['./parse.py', '-f', '{0}/acf/endpoint2'.format(UPTIME_ROOT_DIR)])
    return None

def test_process_rawacfs(conn=sqlite3.connect("superdarntimes.sqlite")):
    """
    Sort of a composite test of fetching a bit of data and processing all files 
    in a directory.

    This method exists specifically to test whether everything's 
    configured properly to run the script to grab an entire month's data, 
    without having to do all of the hauling. 

    :param conn: [sqlite3 connection] to the database
    """

    # Test 1: Globus query
    rut.globus_connect()
    script_query = [rut.SYNC_SCRIPT_LOC,'-y', '2017', '-m',
        '02', '-p', '20170209.0*zho', rut.ENDPOINT]
    rut.globus_query(script_query)

    # Test 2: verify that we can parse this stuff
    parse.parse_rawacf_folder(rut.ENDPOINT, conn=conn )
    logging.info("Done with parsing 2017-02-09 'sas' rawacf data")

    # Test 3: Clear the rawacf files that we fetched
    try:
        rut.clear_endpoint()
        logging.info("Successfully removed 2017-02-09 'sas' rawacf data")

    except subprocess.CalledProcessError:
        logging.error("\t\tUnable to remove files")

# ------------------------------------------------------------------------------
#                       Parse.py Tests: Helper Methods
# ------------------------------------------------------------------------------

def test_err_writers():
    """
    Tests the functions write_bad_rawacf() and write_inconsistent_rawacf().

    [critical test]
    """
    ex1 = rut.BadRawacfError('Test Bad Exception')
    ex2 = rut.InconsistentRawacfError('Test Inconsistent Exception')
    fname = 'testfile'
    cpids_f, rawacfs_f = parse.BAD_CPIDS_FILE, parse.BAD_RAWACFS_FILE
    parse.BAD_CPIDS_FILE, parse.BAD_RAWACFS_FILE = 'test_bad_files.txt', 'test_bad_files.txt'
    write_bad_rawacf(fname, ex1)
    write_inconsistent_rawacf(fname, ex2)
    with open(fname, 'r') as f:
        file_contents = f.read()
        test_str = "testfile: Test Inconsistent Exception\ntestfile: Test Bad Exception"    
        if file_contents != test_str:
            logging.info("test_err_writers() failed!")
            parse.BAD_CPIDS_FILE, parse.BAD_RAWACFS_FILE = cpids_f, rawacfs_f
            return 1 
    parse.BAD_CPIDS_FILE, parse.BAD_RAWACFS_FILE = cpids_f, rawacfs_f
    return 0

def test_init_log():
    """
    Verify that the logger initialization works.

    [noncritical test]
    """
    initialize_logger(False)
    # check...
    initialize_logger(True)
    # check...

def test_exc_handler():
    """
    Tests whether multiprocessing can be used to spawn an exception handler
    process.
    """
    manager = mp.Manager()
    exc_msg_queue = manager.Queue()
    p = mp.Process(target=exc_handler_func, args=( exc_msg_queue,))
    p.start()
    exc_msg_queue.put(Exception("Test"))
    # Check that the the exception was processed by the handler
    time.sleep(parse.SHORT_SLEEP_INTERVAL)
    if not exc_msg_queue.empty():
        logging.error("Exception handler seems to be not doing its job!")
        return 1
    else:
        return 0

# ------------------------------------------------------------------------------
#                   rawacf_utils.py Tests: Database methods
# ------------------------------------------------------------------------------

def test_reads():
    """
    Check that functions that use the backscatter DMAP reads are successful.
    """
    if bz2_dic(TEST_RAWACF) is not None:
        logging.error("Erroneous bz2_dic() result!")
    if acf_dic(TEST_BZFILE1) is not None:
        logging.error("Erroneous acf_dic() result!")
    # Should read successfully
    dics = rut.bz2_dic(TEST_BZFILE)
    if type(dics) != list and type(dics[0]) is not dict:
        logging.error("Erroneous bz2_dic() result!")
    dics2 = rut.acf_dic(TEST_RAWACF) 
    if type(dics2) != list and type(dics2[0]) is not dict:
        logging.error("Erroneous acf_dic() result!")
    try:
        dics3 = rut.bz2_dic(TEST_BZFILE)
    except rut.InconsistentRawacfError:
        # working as intended that this happens
        pass
        
def test_globus():
    """
    Test for checking that globus functions can work
    """
    rut.globus_connect()
    # Try a globus query
    # Try a globus disconnect?
    pass
    rut.globus_disconnect()

def test_db():
    mydb = "testdb.sqlite"
    conn = rut.connect_db(dbname=mydb)
    cur = conn.cursor()
    # Try connecting to a database and enforcing its structure?
    # Try saving to it?
    # Try closing/disconnecting/clearing?

# ------------------------------------------------------------------------------
#                   rawacf_utils.py Tests: Utility Methods
# ------------------------------------------------------------------------------

def test_check_fields():
    """
    Tests whether a list of dmap dictionaries is properly checked by the
    check_fields() function.
    """
    rut.check_fields(test_dmap_dicts) 
    test_dmap_dicts[0]['cp'] = 1
    rut.check_fields(test_dmap_dicts)

    test_dmap_dicts[0]['origin.command'] = "test"
    
def test_records():
    """

    """
    # First and foremost, testing record_from_dics    
    dmap_dicts = rut.acf_dic(TEST_RAWACF) 
    r = rut.RawacfRecord.record_from_dics(dmap_dicts)

if __name__=="__main__":
    rut.read_config()
    rut.globus_connect()
    if 'tests' not in os.listdir(UPTIME_ROOT_DIR):
        subprocess.call('mkdir {0}'.format(TESTS_DIR))
        subprocess.call('mkdir {0}/endpt'.format(TESTS_DIR))
    rut.ENDPOINT =  "{0}/endpt".format(UPTIME_ROOT_DIR)
    conn = rut.connect_db()
    cur = conn.cursor()
    

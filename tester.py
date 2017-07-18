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

import time
import rawacf_utils as rut
import parse
import uptime
import sqlite3
import multiprocessing as mp
import logging

UPTIME_ROOT_DIR = '.'
TESTS_DIR = UPTIME_ROOT_DIR + '/tests'
#UPTIME_ROOT_DIR='..'

TEST_BZFILE1 = 'tests/sample_files/20161201.0401.00.bks.rawacf.bz2'
TEST_BZFILE2 = 'tests/sample_files/20161201.1441.54.han.rawacf.bz2'
TEST_RAWACF = 'tests/sample_files/20170601.0001.00.sas.rawacf'
TESTDB = "testdb.sqlite"

test_dmap_dicts = [{'cp': 3, 'origin.command': 'test', 'stid': 5, 'txpl': 300,
                    'rsep': 45, 'bmnum': 0},
                    {'cp': 3, 'origin.command': 'test', 'stid': 5, 'txpl': 300,
                    'rsep': 45, 'bmnum': 0},
                    {'cp': 3, 'origin.command': 'test', 'stid': 5, 'txpl': 300,
                    'rsep': 45, 'bmnum': 0}]

sample_start_iso = "2017-07-18T15:00:37.245704" 
sample_end_iso = "2017-07-18T15:30:00"

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
    logging.info("Testing correct record-keeping of erroneous files...")
    ex1 = rut.BadRawacfError('Test Bad Exception')
    ex2 = rut.InconsistentRawacfError('Test Inconsistent Exception')
    fname = 'testfile'
    test_listfile = 'test_bad_files.txt'
    parse.write_bad_rawacf(fname, ex1, bad_files_log=test_listfile)
    parse.write_inconsistent_rawacf(fname, ex2, inconsistents_log=test_listfile)
    with open(test_listfile, 'r') as f:
        file_contents = f.read()
        test_str = "testfile:\"Test Bad Exception\"\ntestfile:Test Inconsistent Exception\n"    
        if file_contents != test_str:
            print file_contents
            print test_str
            logging.error("test_err_writers() failed!")
    os.remove(test_listfile)

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
    logging.info("Testing the exception handler from parse.py...")
    manager = mp.Manager()
    exc_msg_queue = manager.Queue()
    p = mp.Process(target=parse.exc_handler_func, args=( exc_msg_queue,))
    p.start()
    exc_msg_queue.put(('testfile',Exception("Test")))
    # Check that the the exception was processed by the handler
    time.sleep(parse.SHORT_SLEEP_INTERVAL)
    if not exc_msg_queue.empty():
        logging.error("Exception handler seems to be not doing its job!")
    p.terminate()

# ------------------------------------------------------------------------------
#                   rawacf_utils.py Tests: Database methods
# ------------------------------------------------------------------------------

def test_reads():
    """
    Check that functions that use the backscatter DMAP reads are successful.
    """
    logging.info("Testing acquisition of dmap lists from bz2/rawacf files")
    # Running bz2_dic and acf_dic on the wrong files should throw exceptions
    try:
        dics = rut.bz2_dic(TEST_RAWACF)
        logging.error("Erroneous bz2_dic() result! 1")
    except IOError: 
        pass

    try:
        dics = rut.acf_dic(TEST_BZFILE1)
        logging.error("Erroneous acf_dic() result! 2")
    except IOError:
        pass

    # Next tests should read successfully
    dics = rut.bz2_dic(TEST_BZFILE1)
    if type(dics) != list and type(dics[0]) is not dict:
        logging.error("Erroneous bz2_dic() result! 3")

    dics2 = rut.acf_dic(TEST_RAWACF) 
    if type(dics2) != list and type(dics2[0]) is not dict:
        logging.error("Erroneous acf_dic() result! 4")
        
def test_globus():
    """
    Test for checking that globus functions can work
    """
    logging.info("Testing connecting, fetching, and disconnecting with globus...")
    rut.globus_connect()

    # Try a globus query
    script_query = [rut.SYNC_SCRIPT_LOC,'-y', '2017', '-m',
        '02', '-p', '20161202.08*zho', rut.ENDPOINT]
    rut.globus_query(script_query)
    if not os.path.isfile(rut.ENDPOINT + '/' + '20161202.0801.00.zho.rawacf.bz2'):
        logging.error("Problem with a globus fetch using globus_query")

    # Try a globus disconnect?
    rut.globus_disconnect()

def test_db():
    """
    Database tests with a separate custom database.
    """
    logging.info("Testing the database functions...")
    # Try connecting to a database and enforcing its structure?
    mydb = TESTDB
    conn = rut.connect_db(dbname=mydb)
    cur = conn.cursor()
    if not os.path.isfile(mydb) or not rut.check_db(cur):
        logging.error("Problem with connecting to or creating DB!")

    # Try saving to it?
    cur.execute('INSERT INTO exps (stid, start_iso, end_iso) VALUES (?, ?, ?)',
                (3, sample_start_iso, sample_end_iso))
    conn.commit()

    # Try accessing it? 
    test_sql = 'select * from exps'
    r = rut.select_exps(test_sql, cur)
    if len(r) != 1 and not isinstance(rut.RawacfRecord, r):
        logging.error("Problem with database insert/retrieval!")

    # Try closing/disconnecting/clearing?
    rut.dump_db(conn)
    r = rut.select_exps(test_sql, cur)
    if r != []:
        logging.error("Problem with dumping database!")   

# ------------------------------------------------------------------------------
#                   rawacf_utils.py Tests: Utility Methods
# ------------------------------------------------------------------------------

def test_check_fields():
    """
    Tests whether a list of dmap dictionaries is properly checked by the
    check_fields() function.
    """
    logging.info("Testing the field-checking for dmap entries...")
    objection_dict = rut.check_fields(test_dmap_dicts) 
    test1 = objection_dict.keys() == []

    # Needs to be able to deal with a non-splittable command name without breaking 
    test_dmap_dicts[0]['origin.command'] = "test"
    objection_dict = rut.check_fields(test_dmap_dicts) 
    test2 = objection_dict.keys() == []

    # Needs to detect cpid inconsistency
    tmp = test_dmap_dicts[0]['cp']
    test_dmap_dicts[0]['cp'] = 1
    objection_dict = rut.check_fields(test_dmap_dicts)
    test3 = 'cp' in objection_dict.keys()
    test_dmap_dicts[0]['cp'] = tmp

    # Needs to detect unreasonable beam values
    test_dmap_dicts[0]['bmnum'] = 27
    objection_dict = rut.check_fields(test_dmap_dicts)
    test4 = 'bmnum' in objection_dict.keys()
   
    # Needs to detect incorrect relationship between rsep and txpl
    test_dmap_dicts[0]['rsep'] = 50
    test_dmap_dicts[0]['txpl'] = 250
    objection_dict = rut.check_fields(test_dmap_dicts)
    test5 = 'rsep' in objection_dict.keys()
    if not(test1 and test2 and test3 and test4 and test5):
        logging.error("Problem wth check_fields()!")

def test_records():
    """
    Tests the creation of RawacfRecord objects and their use.

    Full coverage would require special object cases etc., but for here
    I'm just double-checking that the basic case-handling works.
    """
    # Testing record_from_dics()  
    dmap_dicts = rut.acf_dic(TEST_RAWACF) 
    r = rut.RawacfRecord.record_from_dics(dmap_dicts)
    logging.debug("Test record built from dmap dictionaries:\n{0}".format(r))
    if type(r) != rut.RawacfRecord:
        logging.error("Problem with record_from_dicts")
 
    # Testing save_to_db()
    conn = rut.connect_db(TESTDB)
    cur = conn.cursor()
    r.save_to_db(cur)

    # Testing select_exps (again)
    # recs = rut.select_exps('select * from exps', cur)
    
    # Testing record_from_tuple()
    cur.execute('select * from exps')
    recs = cur.fetchall()
    tup = recs[0]
    if type(tup) != tuple:
        err_str = "Unexpected result in attempting to test record_from_tuple."
        err_str += " Possible problem with save_to_db()"
        logging.error(err_str)
    r2 = rut.RawacfRecord.record_from_tuple(tup)
    logging.debug("Test record after saving to DB and reconstructing:\n{0}".format(r2))
    if type(r2) != rut.RawacfRecord:
        logging.error("Problem with record_from_tuple")
    
    # Testing duration()
    dur = r.duration()
    logging.debug("Duration of test record: {0}".format(dur))
    if type(dur) != float:
        logging.error("Error with duration()")

if __name__=="__main__":
    rut.read_config()
    rut.globus_connect()
    if 'tests' not in os.listdir(UPTIME_ROOT_DIR):
        subprocess.call('mkdir {0}'.format(TESTS_DIR))
        subprocess.call('mkdir {0}/endpt'.format(TESTS_DIR))
    rut.ENDPOINT =  "{0}/endpt".format(UPTIME_ROOT_DIR)
    conn = rut.connect_db()
    cur = conn.cursor()
   
    parse.initialize_logger(quiet_mode=False)#True)
    test_reads()
    test_check_fields() 
    test_db()
    test_records() # Requires reads(), fields(), db() to have been tested before.

    test_exc_handler()
    test_err_writers()

    #test_process_rawacfs()

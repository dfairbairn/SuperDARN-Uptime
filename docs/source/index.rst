.. uptime documentation master file, created by
   sphinx-quickstart on Fri Jul  7 14:58:16 2017.
   You can adapt this file completely to your liking, but it should at least
   contain the root `toctree` directive.

Welcome to uptime's documentation!
++++++++++++++++++++++++++++++++++

Introduction
============
This project provides scripts for fetching and processing SuperDARN experiment 
metadata and calculating basic statistics from these processed records.

A detailed log of events during fetching, parsing, and processing is maintained 
as well as a separately compiled list of SuperDARN .rawacf files which showed 
signs of data corruption or otherwise unusual behaviour.

Installation
============
This script uses a number of basic Python packages as well as two crucial 
specialized modules. It's highly recommended to make use of the python Virtual
Environments package 'virtualenv' so as to create a convenient local 
environment for this script.
Read more here: http://docs.python-guide.org/en/latest/dev/virtualenvs/

Specialized module #1 is *backscatter*. It's used for its ability to read
.rawacf files and retrieve their metadata (backscatter.dmap). Follow 
installation procedure here: http://backscatter.readthedocs.io/en/latest/

Specialized module #2 is the *sync_radar_data_globus.py* script. This script
is used to perform data fetch requests in order to bring in, parse, and save
SuperDARN record metadata to a database locally. 

**This requires that you
have a Globus account with access to the SuperDARN Globus endpoints, with 
Globus configured locally!** 

In terms of the necessary steps for installation:
[1] 
Get virtualenv and create a virtual environment, and activate it.
> virtualenv uptime
> source uptime/bin/activate

[2] 
The necessary python modules are listed below. 

- numpy

- sqlite3 (or 'pysqlite')

- calendar (or 'cal')

- dateutil (or 'python-dateutil')

- configparser

- multiprocessing

They can be installed by running: 
> pip install -r docs/requirements.txt

[3] 
The specialized packages you'll need are:

- backscatter (see here: https://github.com/SuperDARNCanada/backscatter )
    * Note: this is already included in the requirements.txt file 

- globus (see here:  https://github.com/SuperDARNCanada/globus )
    * Note: this _must_ be downloaded deliberately, but it's okay 'cause it's easy

[4]
For parsing or fetching, you'll need a config file set-up which points to
the location of the globus script from step 3. If you run rawacf_utils.read_config(),
a 'sample_config.ini' file will be created which explains the structure required. 


Usage
=====
The files 'parse.py' and 'uptime.py' are written so as to be usable either
from the command-line or from other python scripts. The python package 
'argparse' was used, which includes help prompts at the command-line.

A script 
Example usage of uptime.py
--------------------------
Use of 'uptime.py' for calculating uptime statistics (assuming the 
'superdarntimes.sqlite' file has already been filled with records for the 
desired period using parse.py) is done like so:


> uptime.py -y 2017 -m 3 -d 28 -i 5

Uses uptime.py's "stats_day()" method to take a SuperDARN radar ID and a 
specific year, month, and day, looks through all the SuperDARN records in the 
superdarntimes.sqlite database, and computes the uptime for that day. SuperDARN 
radar IDs are numbers as shown here: 
http://superdarn.ca/news/item/58-sd-radar-list 


> uptime.py -y 2017 -m 3 -i 5

Similar to the previous example, but uses uptime.py's "stats_month()" which
calls multiple runs of "stats_day()"

Example usage of parse.py
-------------------------
Command-line usage of 'parse.py' for fetching and processing SuperDARN record
data is done like so:

> parse.py -y 2017 -m 3

This calls parse.py's method "process_rawacfs_month()" with 2017, 3, as
parameters for year, and month, respectively. This method will iterate through
each day in the month of 2017-03, requesting _all_ SuperDARN .rawacf for that
day.


> parse.py -y 2017 -m 3 -d 29 -c sas

This run is comparable to the previous, but illustrates that two optional 
parameters can be provided to fetch and process a smaller dataset. The -d
option specifies a particular day of the month, while the -c option can be used
to specify a particular SuperDARN radar code (for SuperDARN data codes, see
here: http://superdarn.ca/news/item/58-sd-radar-list


> parse.py -f data/20170601.2001.00.cly.rawacf.bz2

This calls parse.py's method "parse_file()" on the specified file, which
will only read the file and save its metadata to the superdarntimes.sqlite 
database.


> parse.py -d data/

This calls parse.py's method "parse_rawacfs_folder()" on the specified 
directory, which will only read the files already in the directory and save 
their metadata to the superdarntimes.sqlite database.

Current Issues and Necessary Work
=================================
I) To massively improve the time it takes to perform parsing and processing of
the .rawacf files, the multiprocessing package for python was used. However,
it seems to possibly be causing errors. For now, sequential processing is 
performed, which generally takes ~14 hours for one month's data to be analyzed.

II) Some regular errors are frequently encountered in the execution of these
scripts. Certain .rawacf files can't be parsed by *backscatter* so database
entries for these records will be absent (when perhaps there was only a minor
formatting error).

.. toctree::
   :maxdepth: 2
   :caption: Contents:

Indices and tables
==================

* :ref:`genindex`
* :ref:`modindex`
* :ref:`search`

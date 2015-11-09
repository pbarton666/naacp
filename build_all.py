import build_flat_files
import build_tables

import logging
import logging.handlers

#*******change these to your own settings (dirs must exist)***

#path to the data as downloaded from DropBox
#NB: I'm using shortened versions of the scenario directory names
#    as these are part of the column names in the DB

#Data directories and output directories
dirs = [#{'data': '/home/pat/mary/NoRed-YesHwy',  'scratch': '/home/pat/mary/flat_NoRed-YesHwy'},
        #{'data': '/home/pat/mary/NoRed-NoHwy',   'scratch': '/home/pat/mary/flat_NoRed-NoHwy'},
        #{'data': '/home/pat/mary/YesRed-YesHwy', 'scratch': '/home/pat/mary/flat_YesRed-YesHwy'},
        #{'data': '/home/pat/mary/YesRed-NoHwy',  'scratch': '/home/pat/mary/flat_YesRed-NoHwy'},
        {'data': '/home/pat/mary/test',  'scratch': '/home/pat/mary/flat_test'},
        ]

#database name
DB='naacp'
#log file name - lives in the script directory
LOG_FILE='loader.log'
#set to 'WARN' to capure only data loading issues.  'INFO' is verbose.
LOG_LEVEL='INFO'   

#**** login credentials need to be updated in database.py ***


#**********************************************************

logger = logging.getLogger('trans_logger')
h = logging.handlers.RotatingFileHandler(LOG_FILE, 
                                         'a', 
                                         maxBytes=10*1024*1024, 
                                         backupCount=5)
formatter = logging.Formatter('%(asctime)s - %(levelname)s - '
                              '%(filename)s:%(lineno)s - %(message)s',
                              datefmt="%Y-%m-%d %H:%M:%S")
h.setFormatter(formatter)
logger.addHandler(h)
logger.setLevel(LOG_LEVEL)




if __name__=='__main__':
    'main execution start'
    #leave these statements here - logging info imported from settings
    for ix, d in enumerate(dirs, start=1):
        data_dir=d['data']
        scratch_dir = d['scratch']
        msg='Data loading from {} \n...to database {}. \n...Logging to {} \n'
        print(msg.format(data_dir, DB, LOG_FILE))
        #create the flat files for import
        build_flat_files.build_flat_files(data_dir, scratch_dir)
        #create tables from header info in tables, then load data
        build_tables.build_tables(db=DB, pdir = scratch_dir, drop_old=True)
        if ix != len(dirs):
            logger.info("*******************************")
            logger.info("Beginning new scenario")
            logger.info("*******************************")
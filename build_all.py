import build_flat_files
import build_tables

import logging
import logging.handlers

#*******change these to your own settings (dirs must exist)***
DATA_DIR='/home/pat/mary/NoRed-YesHwy'
OUT_DIR = '/home/pat/mary/flat_NoRed-YesHwy'
DB='naacp'
LOG_FILE='loader.log'
LOG_LEVEL='INFO'   #set to 'WARN' to capure only data loading issues

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
    msg='Data loading from {} \n...to database {}. \n...Logging to {} \n'
    print(msg.format(DATA_DIR, DB, LOG_FILE))
    #create the flat files for import
    build_flat_files.build_flat_files(DATA_DIR, OUT_DIR)
    #create tables from header info in tables, then load data
    build_tables.build_tables(db=DB, pdir = OUT_DIR, drop_old=True)
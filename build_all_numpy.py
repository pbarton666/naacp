import build_flat
import build_tables

import logging
import logging.handlers

#*******change these to your own settings******************
data_dir='/home/pat/data/RL_analysis/raw_data_unzipped'
out_dir = '/home/pat/data/RL_analysis/raw_data_output'
DB='naacp'

#Also, login credentials need to be updated in database.py
#**********************************************************

logger = logging.getLogger('trans_logger')
h = logging.handlers.RotatingFileHandler('loader.log', 
                                         'a', 
                                         maxBytes=10*1024*1024, 
                                         backupCount=5)
formatter = logging.Formatter('%(asctime)s - %(levelname)s - '
                              '%(filename)s:%(lineno)s - %(message)s',
                              datefmt="%Y-%m-%d %H:%M:%S")
h.setFormatter(formatter)
logger.addHandler(h)
logger.setLevel("INFO")


if __name__=='__main__':
    #create the flat files for import
    build_flat.create_np_array(data_dir, out_dir)
    #create tables from header info in tables, then load data
    build_tables.build_tables(db=DB, pdir = out_dir, drop_old=True)
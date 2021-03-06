"""This module builds new database tables using flat files produced by build_flat_files.py.
   The main routine is build_tables().  Usage:
   
   build_tables(<data_base_name>, <flat_file_direcotry>, <drop_old>)
       The <drop_old> parameter DROPS existing db table
   """
import os
import logging
import string
import psycopg2
import logging
import logging.handlers
from pprint import pprint

from database import login_info
import utils_and_settings

logger = logging.getLogger('trans_logger')

#some constants testing purposes
DEBUG=True
DB = 'test'

if DEBUG:
    PARENT_DIR ='/home/pat/data/RL_analysis/test_output'
    DB='test'
else:
    PARENT_DIR= '/home/pat/data/RL_analysis/raw_data_unzipped'
    DB='naacp'

def build_tables(db = DB, pdir=PARENT_DIR, drop_old=True, data_dir=None):
    """Builds database tables from flat files produced by build_flat_files.py.
       Assumes that column names are in a header row, in a specific format:
       #  <col name>|<col name>| (etc)
       
       Also assumes the db table is all integers.
       
       First, it attempts to use COPY to create the tables and load the data.
       This may fail with large files, depending on memory available.  If this 
       happens, it reverts to using INSERT statements.
    """
    conn = psycopg2.connect(database = db,
                            user=login_info['user'],
                            password=login_info['password']
                            )
    curs = conn.cursor()    
  
    #if the database is not available, try to create it
    try:
        curs.execute('END')
        curs.execute('CREATE DATABASE {}'.format(db))
        logger.debug('creating new database')
    except psycopg2.ProgrammingError:      
        pass  #datbase already exists

    #With partitioned files, make sure that the table is only deleted once
    #  We'll use this to remember which have been DROPped
    safe_tables=set()
    
    #find each file in the data dir, create a db table, and load the data
    for root, dirs, files in os.walk(pdir):
        for f in files:
            
            logging.info('building table for file {}'.format(f))
            #the table name comes from the file name
            t_name = utils_and_settings.get_table_name_from_fn(f)
            
            #DROPs existing table. 
            if drop_old and not t_name in safe_tables:
                logger.debug('dropping table {}'.format(t_name))
                curs.execute('END')
                sql = 'DROP TABLE IF EXISTS {}'.format(t_name)
                curs.execute(sql)
                logger.debug('dropping table {}'.format(t_name))
                safe_tables.add(t_name)
                

            logger.debug('creating new table {}'.format(t_name))

            #get the header contains basis for col names. Example: # origin|dest|col1name|col2
            file=os.path.join(root, f)
            with open(file, 'r') as fil:
                header = fil.readline() 
                cols =header.replace('#','').strip().split('|')
                db_cols=[]
                for c in cols:
                    db_cols.append(c)                 
                    
                #create a table based on the columns (assume all fields float)
                sql='CREATE TABLE IF NOT EXISTS {} (\n'.format(t_name)
                for col in db_cols:
                    sql+='\t {} float,\n'.format(col)
                #for col in db_cols[:2]:   #origin, destination
                    #sql+='\t {} integer,\n'.format(col)
                #for col in db_cols[2:]:
                    #if data_dir in utils_and_settings.integer_dirs:
                        #sql+='\t {} integer,\n'.format(col)       
                    #else:
                        #sql+='\t {} float,\n'.format(col) 
                sql = sql[:-2] + ');' #
                curs.execute(sql)
                conn.commit()  
                logger.info('Created table {}'.format(t_name))
                logger.debug('Columns: {}'.format(db_cols))
                
                #try to load the file with COPY
                logger.debug('Loading flat file {} to the database with COPY'.format(f))
                fil.seek(0)
                cols = '(' + ', '.join(db_cols) + ')'
                sql="COPY {} {} FROM STDIN WITH CSV HEADER DELIMITER AS ','".format(t_name, cols) 
                try:
                    curs.copy_expert(sql=sql, file=fil)    
                    conn.commit()
                    logger.debug('success.')
                except: 
                    #failed - probably due to memory issues; do it the slow way with INSERTs
                    logger.warning('Nope.  COPY failed for file {} trying to INSERT it.'.format(f))                    
                    load_with_insert(db=db, t_name=t_name, file=file, drop_old=drop_old)    

                  
def load_with_insert(db=None, t_name=None, file=None, drop_old=None):
    "loads db with INSERT method.  Slow but foolproof" 
    
    conn = psycopg2.connect(database = db,
                            user=login_info['user'],
                            password=login_info['password']
                            )
    curs = conn.cursor()        
    
    if drop_old:
        curs.execute('DROP TABLE IF EXISTS {}'.format(t_name))
        logger.debug('dropping table {}'.format(t_name))

    with open(file, 'r') as fil:
        header = fil.readline() 
        cols =header.replace('#','').strip().split('|')
        db_cols=[]
        for c in cols:
            db_cols.append(utils_and_settings.get_column_name(c))                 
            
        #create a table based on the columns (assume all fields int)
        sql='CREATE TABLE IF NOT EXISTS {} ('.format(t_name)
        for col in db_cols:  
            sql+=' {} integer,'.format(str(col))        
        sql = sql[:-1] + ');' #
        #print(sql)
        curs.execute(sql)
        conn.commit()  

        logger.debug('Loading flat file {} to the database with INSERTS'.format(t_name))

        for line in fil.readlines():
            sql="INSERT INTO {} (".format(t_name, cols) 
            for col in db_cols:  
                sql+=' {}, '.format(col)   
            sql=sql[:-2] + ") VALUES ("
            for value in line.split(','):
                sql+=' {},'.format(value)            
            sql=sql[:-2] + ") "
            #print(sql)
            curs.execute(sql)  

        conn.commit()
        logger.debug('success.')    
        conn.close()
    return t_name

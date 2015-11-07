"""This module builds new database tables to accomodate model
   output that exists in the subdirs associated w/ some parent dir.

   Table names determined by the directory name.  Can be customized with 
   'dir_map' below.

   Columns are added for each data file in the directory.  Fields are assumed to
   be integers.

   Maintenance: to add data create the same directory structure in a separate
   folder with the new data files, using a different PARENT_DIR.  File naming conventions
   should be consistent with existing ones.  (Especially in that the bit preceeding the 
   first _ is not relevant to the contents and is ignored here.)


   """
import os
import logging
import string

from database import login_info
import psycopg2
import logging
import logging.handlers
import utilities

logger = logging.getLogger('trans_logger')

#some constants for stand-alone mode (normally provided by run_all)
DEBUG=False

DB = 'test'
FN_DELIMITER = '_'  #info about matrices is stored in the file names themselves.

if DEBUG:
    PARENT_DIR ='/home/pat/data/RL_analysis/test_output'
    DB='test'
else:
    PARENT_DIR= '/home/pat/data/RL_analysis/raw_data_unzipped'
    DB='naacp'

def build_tables(db = DB, pdir=PARENT_DIR, drop_old=True):
    "build tables based on dir contents, starting at root"   

    conn = psycopg2.connect(database = db,
                            user=login_info['user'],
                            password=login_info['password']
                            )
    curs = conn.cursor()    

    origin='origin'
    destination='destination'

    try:
        curs.execute('END')
        curs.execute('CREATE DATABASE {}'.format(db))
        logger.info('creating new database')
    except psycopg2.ProgrammingError:
        #datbase already exists
        pass

    #each file contains the data for a single table
    for root, dirs, files in os.walk(pdir):
        for f in files:
            t_name = utilities.get_table_name(os.path.splitext(f)[0])

            if drop_old:
                logger.info('dropping table {}'.format(t_name))
                curs.execute('END')
                sql = 'DROP TABLE IF EXISTS {}'.format(t_name)
                curs.execute(sql)
                logger.info('dropping table {}'.format(t_name))

            logger.info('creating new table {}'.format(t_name))

            #get the header 
            with open(os.path.join(root,f), 'r') as fil:
                #grab the header and process it (put into db-friendly format)
                header = fil.readline() # origin|dest|great_junk100|great_junk200
                cols =header.replace('#','').strip().split('|')
                db_cols=[]
                
                #create a table based on the columns (assume all fields int)
                for c in cols:
                    db_cols.append(utilities.get_column_name(c))
                sql='CREATE TABLE IF NOT EXISTS {} (\n'.format(t_name)
                for col in db_cols:
                    sql+='\t {} integer,\n'.format(col)
                sql = sql[:-2] + ');' #lose last comma, add closing )
                curs.execute(sql)
                conn.commit()  
                #s='CREATE TABLE IF NOT EXISTS pat.public.dir_1_data (\n\t origin integer,\n\t dest integer,\n\t junk100 integer,\n\t junk200 integer);'
                #conn.commit()
                fil.seek(0)
                cols = '(' + ', '.join(db_cols) + ')'
                sql="COPY {} {} FROM STDIN WITH CSV HEADER DELIMITER AS ','".format(t_name, cols) 
                logger.info('copying flat file {} to the database'.format(t_name))
                curs.copy_expert(sql=sql, file=fil)    
                logger.info('success.')
                conn.commit()
                
                if DEBUG:
                    curs.execute('select * from {}'.format(t_name))
                    print('*'*100)
                    for r in curs.fetchall():
                        print(r)

def load_files(pdir=None, db=DB, clean_up_dir=True) : 
    """Loads flat files to a table, optionally retaining the flat file.
    Sweeps up all the flat files in a directory, 
        """  
    ##TODO Can be run stand-alone when new data is available (use INSERTs)

    conn = psycopg2.connect(database = db,
                            user=login_info['user'],
                            password=login_info['password']
                            )
    curs = conn.cursor()    

    with open(file, 'r') as f:
        #pick off the header and move pointer back to start
        cols='({})'.format(f.readline())
        f.seek(0)
        sql = "COPY {} {} FROM STDIN WITH CSV HEADER".format(table, cols)
        curs.copy_expert(sql=sql, file=f)   
        logger.info('Loaded file {} into table {}'.format(file, table))


    conn.commit()
    conn.close()






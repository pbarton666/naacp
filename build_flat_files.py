"""The data is organized in directories, then in files.  The directory name gives
       away the name of the table; the file name implies the name of the column.
       
       We'll read the entire contents of a directory into a np array, ready to save off
       into a file for a COPY operation on the db.  Separately, we'll need to keep an
       ordered list of the file names for creating the db tables.  The COPY import operation
       allows importing files with headers, but the db doesn't bother reading them.  Data rows
       have to appear in the same column order as the data table itself.  """

from database import login_info
import psycopg2
import os
import csv
import logging
import logging.handlers
import shutil
import numpy as np
from pprint import pprint

#from build_tables import dir_map, get_column_name, get_table_name, load_file
from settings import *
from utilities import get_column_name, get_table_name, make_OD_array, name_to_flat

logger = logging.getLogger('trans_logger')

def create_np_array(in_dir, out_dir):
    """Given a directory, gathers all file contents into an array and saves it.
       The first two columns will have the origin (row) and destination (col)"""
    
    np.set_printoptions(precision=4)
    #comb thru and find the files and directories
    have_rows=False

    for root, dirs, files in os.walk(in_dir):
        #each dir will contain files for a single array
        for d in dirs:
            logger.info('creating np array from {}'.format(d))
            #figure out which db table
            table_name=get_table_name(d) 
            #what files?
            files=os.listdir(os.path.join(root,d))
            cols= len(files) + 2
            columns_reported = ['origin', 'dest']
            #rows and cols for main array 
            if (not have_rows) and files:
                data = np.genfromtxt(os.path.join(root, d, files[0]), delimiter=',', dtype=int)
                rows=len(data)-1

            #our main np array
            npa=np.zeros((rows**2, cols))   #all zero arr
            od = make_OD_array(rows)
            #replace the first two cols with O D values
            npa[:,0]=od[0]
            npa[:,1]=od[1]              
            
            #the column for the data (first 2 are for O/D)
            data_col=2
            
            for f in files:
                logger.info('working on file: {}'.format(f))
                fn = os.path.join(root, d, f)
                data = np.genfromtxt(fn, delimiter=',', dtype=int)
                #rows=len(data)-1
                data = data[1:, 1:]          #removes row/col headers
                data=data.ravel()            #turns it into a vector

                #... and the next with data
                npa[:,data_col]=data[:]
                columns_reported.append(get_column_name(f))
                
                data_col+=1

            #save the file 
            data_fn=os.path.join(root, out_dir, d+"_data.csv")
            #print('data:', data_fn)            
            np.savetxt(data_fn, npa, delimiter=',', header="|".join(columns_reported), fmt="%u")
            #pprint(npa)
            logger.info('done')

                
    a=1
            
            


"""The data is organized in directories, then in files.  The directory name gives
       away the name of the table; the file name implies the name of the column.
       
       We'll read the entire contents of a directory into a np array, ready to save off
       into a file for a COPY operation on the db.  """

from database import login_info
import psycopg2
import os
import csv
import logging
import logging.handlers
import shutil
import numpy as np
from pprint import pprint

from utils_and_settings import get_column_name, get_table_name_from_dir, make_OD_array, name_to_flat

logger = logging.getLogger('trans_logger')

def build_flat_files(in_dir, out_dir):
    """Given a directory, gathers all file contents into an array and saves it.
       The first two columns will have the origin (row) and destination (col)"""
    
    np.set_printoptions(precision=4)
    data_file_lengths=[]
    
    if not os.path.exists(out_dir):
        os.mkdir(out_dir)  

    for root, dirs, files in os.walk(in_dir):
        #each dir will contain files for a single array
        for d in dirs:
            logger.info('creating np array from {}'.format(d))
            #figure out which db table #this will be the last bit of the root + current           
            table_name=get_table_name_from_dir(os.path.join(root, d) )
            #what files?
            files=os.listdir(os.path.join(root,d))
            cols= len(files) + 2
            columns_reported = ['origin', 'dest']
            #rows and cols for main array 
            if files:
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
            files.sort()
            
            for f in files:
                logger.info('working on file: {}'.format(f))
                fn = os.path.join(root, d, f)
                data = np.genfromtxt(fn, delimiter=',', dtype=int)
                data = data[1:, 1:]          #removes row/col headers
                data=data.ravel()            #turns it into a vector
               #check if this is a null (all zeros) file
                if not data.sum():  
                    logger.warning('**** WARN:  Did not load {} - all 0s ***'.format(os.path.join(d, f)))
                    continue
                
                #check if this is of a different size than others encountered
                file_length = len(data) 
                trial_data_lengths = data_file_lengths[:]
                trial_data_lengths.append(file_length)
                if data_file_lengths and len(set(trial_data_lengths)) > 1:
                    msg = '**** WARN: Did not load {} - expected length of {}, but is was {}.'
                    logger.warning(msg.format(os.path.join(d, f), data_file_lengths[0], file_length))
                    continue
                
                #load data into the next column of the np array (unless it's all zeros)
                npa[:,data_col]=data[:]
                columns_reported.append(get_column_name(f))
                data_file_lengths.append(len(data))
                
                data_col+=1

            #save the file 
            data_fn=os.path.join(root, out_dir, table_name + "_data.csv")
            #print('data:', data_fn)            
            np.savetxt(data_fn, npa, delimiter=',', header="|".join(columns_reported), fmt="%u")
            #(npa)
            logger.info('Done.  File saved as {}'.format(data_fn))

            
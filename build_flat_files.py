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

def find_max_rows(cols):
    "finds how many rows we can create at once, knocking off a few at a time"
    incr=100
    rows=0
    while True:
        rows+=incr
        try:
            npa=np.zeros((rows**2, cols), dtype=int)  
        except:
            break   
    return rows-incr

def build_flat_files(in_dir, out_dir, test_max_rows=None):
    """Given a directory, gathers all file contents into an array and saves it.
       The first two columns will have the origin (row) and destination (col)"""
    
    np.set_printoptions(precision=4)
    data_file_lengths=[]
    
    if not os.path.exists(out_dir):
        os.mkdir(out_dir)  

    for root, dirs, files in os.walk(in_dir):
        #each dir will contain files for a single array
        for d in dirs:
            logger.debug('creating np array from {}'.format(d))
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
                initial_rows = rows
                
            #can we make a big enough array (depends on the computer)?
            if not test_max_rows:
                max_rows=1e10
            try:
                npa=np.zeros((rows**2, cols), dtype=int)
            except:
                #nope.  Let's see what we *can* do.
                start_row=1
                rows = find_max_rows(rows, cols)

            #this is an override to facilitate testing - not used in practice
            if test_max_rows:
                rows = test_max_rows
            
            #we'll need to iterate this block as many times as required to get all the rows
            rows_so_far = 1  #we'll skip the row header
            while rows_so_far < initial_rows:           
                npa=np.zeros((rows**2, cols), dtype=int)   #all zero arr
                od = make_OD_array(rows)
                #replace the first two cols with O D values
                npa[:,0]=od[0]
                npa[:,1]=od[1]              
                
                #the column for the data (first 2 are for O/D)
                data_col=2
                files.sort()
                
                for f in files:
                    logger.debug('working on file: {}'.format(f))
                    fn = os.path.join(root, d, f)
                    raw_data = np.genfromtxt(fn, delimiter=',', dtype=int, skip_header=rows_so_far)
                    data = raw_data[:, 1:]          #removes row/col headers
                    data=data.ravel()            #turns it into a vector
                   #check if this is a null (all zeros) file - we can only do this on files ingested intact
                    if not data.sum() and rows==initial_rows:  
                        logger.warning('**** WARN:  Did not load {} - all 0s ***'.format(os.path.join(d, f)))
                        continue
                    
                    #check if this is of a different size than others encountered ((intact ingestion))
                    if rows==initial_rows: 
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
                #data_fn=os.path.join(root, out_dir, table_name + "_data.csv")
                #save the data file, labeled with the 'rows_so_far'
                data_fn=os.path.join(root, out_dir, table_name + "_data" + str(rows_so_far) + ".csv")
                msg='attempting to save flat file {} for info in {}'
                logger.debug(msg.format(data_fn, os.path.join(root,d)))
                try:
                    np.savetxt(data_fn, npa, delimiter=',', header="|".join(columns_reported), fmt="%u")
                except:
                    msg='Could not save file {} file {} for info in {}'
                    logger.warning(msg.format(data_fn, os.path.join(root,d)))
                logger.debug('success')
                logger.debug('Done.  File saved as {}'.format(data_fn))
            
                #end of while loop cleanup - send it around again if not doneH
                rows_so_far+= max_rows
            
            
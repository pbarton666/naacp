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
from utils_and_settings import get_window, integer_dirs
logger = logging.getLogger('trans_logger')

def find_max_rows(input_cols, output_cols):
    "finds how many rows we can create at once, adding a few at a time"
    incr=100
    rows=0
    while True:
        rows+=incr
        try:
            npa=np.zeros((rows*input_cols, output_cols), dtype=dtype)  
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
            logger.info('creating np array from {}'.format(d))
            ##determine np array type
            #if d in integer_dirs:
                #dtype = int
            #else:
                #dtype = float
            ##TODO: might want to build record table for arrays (mixed types)
            dtype=float
            #figure out which db table #this will be the last bit of the root + current           
            table_name=get_table_name_from_dir(os.path.join(root, d) )
            #what files?
            files=os.listdir(os.path.join(root,d))
            output_cols= len(files) + 2
            #columns_reported = ['origin', 'dest']
            #rows and cols for main array 
            if files:
                data = np.genfromtxt(os.path.join(root, d, files[0]), delimiter=',', dtype=dtype)
                input_cols=int(data[0][-1])
                input_rows=len(data)-1        #as the data would have it
                initial_rows = input_rows     #remember this for posterity
                rows=initial_rows             #this may change with large directories
                
            #can we make a big enough array (depends on the computer)?
            else:
                print("Sorry, I can't find any files in directory {}".format(os.path.join(root,d)))
                continue
            
            try:
                npa=np.zeros((input_rows*input_cols, output_cols), dtype=dtype)
            except:
                #nope.  Let's see what we *can* do.  First, provide some data to the user
                error_info=[]
                error_info.append('problem creating np array for {}'.format(os.path.join(root,d)))
                msg='input_rows: {}  input_cols: {}  output_cols: {}'
                error.info.append(msg.format(input_rows, input_cols, output_cols ))
                logger.info(error_info)
                
                #                
                rows = find_max_rows(input_columns, output_cols)
                logging.info('files in {} are too big for single load.  Doing it piecemeal'.format(d))

            #this is an override to facilitate testing - not used in practice
            if test_max_rows:
                rows = test_max_rows
            
            #we'll need to iterate this block as many times as required to get all the rows
            rows_so_far = 0              
            #a generator to calculate header and footer rows to skip
            windows= get_window(initial_rows, rows_per=rows, start=2)             
            
            
            while rows_so_far < initial_rows:           
                
                columns_reported = ['origin', 'dest']
                if rows_so_far>=1000:
                    a=1
                rows_needed = min(rows*input_cols, (initial_rows-rows_so_far)*input_cols)
                #print('needed {}'.format(rows_needed))
                npa=np.zeros((rows_needed, output_cols), dtype=dtype)   #all zero arr
                od = make_OD_array(rows, cols=input_cols, start_row=rows_so_far+1, max_row=initial_rows)

                #replace the first two cols with O D values
                npa[:,0]=od[0]
                npa[:,1]=od[1]              
                
                #the column for the data (first 2 are for O/D)
                data_col=2
                files.sort()
                
                this_window=next(windows)     #sets header and footer parts of file to skip
                skip_header = this_window['skip_header']
                skip_footer = this_window['skip_footer']+1
                ##TODO fix this 'last file indexing issue correctly
                if skip_footer==1:
                    skip_footer=0
                
                for f in files:
                    logger.debug('working on file: {}: beginning at line {}'.format(f, skip_header))
                    fn = os.path.join(root, d, f)
                    raw_data = np.genfromtxt(fn, delimiter=',', dtype=dtype, 
                                             skip_header=skip_header,
                                             skip_footer=skip_footer)
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
    
                #save the output file 
                #data_fn=os.path.join(root, out_dir, table_name + "_data.csv")
                #save the data file, labeled with the 'rows_so_far'
                data_fn=os.path.join(root, out_dir, table_name + "_data" + str(rows_so_far) + ".csv")
                msg='attempting to save flat file {} for info in {}'
                logger.debug(msg.format(data_fn, os.path.join(root,d)))
                try:
                    np.savetxt(data_fn, npa, delimiter=',', header="|".join(columns_reported), fmt="%s")
                except:
                    msg='Could not save file {}  for info in {}'
                    logger.warning(msg.format(data_fn, d))
                logger.debug('success')
                rows_so_far += rows

            
            
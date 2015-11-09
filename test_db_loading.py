'''Unittests for transportation data upload scripts'''

import unittest
import psycopg2
import tempfile
import os
import numpy as np
from pprint import pprint
import shutil
import logging
import logging.handlers
import csv

from  database import login_info
import build_flat_files
import build_tables
import utils_and_settings
import build_all #pulling this in for the logger

TEST_FILE_BASE = 'TRANS_OD_table'

DB='test'
conn = psycopg2.connect(database= DB,
    user=login_info['user'],
    password=login_info['password']
    )
curs = conn.cursor() 

#install a test db if needed
try:
    curs.execute('CREATE DATABASE {}'.format(DB))
    conn.commit()
except:
    pass #db already created

logger = logging.getLogger('trans_logger')


def writeFile( subdir, i):
    """Creates a file with headers and data elements this format: <data series><row><col>.   We can 
       therefore create a file named something like "TRANS_OD__100" by invoking this method with i=100.  
       When we pull this file into a db we can quickly figure out that we've got the right data from the right file

       So, for data series 100 (passed in as the 'i' argument), it looks like:
        
    # header <colname_1>, <column_name_2>, <column_name_3>, <column_name_4>
    0        1      2        3      4
    1    10011  10012    10013  10014
    2    10021  10022    10023  10024
    3    10031  10032    10033  10034
    4    10041  10042    10043  10044
    """
    
    fn=TEST_FILE_BASE
    file=fn + str(i)
    with open(os.path.join(subdir,file), 'w') as f:
        f.write(" {},  {},  {},  {},    {}   \n".format(' ',    ' 1',     '2',     '3',      '4' ))
        f.write(" {},  {},  {},   {},   {}  \n".format('1', str(i)+str(11), str(i)+str(12), str(i)+str(13), str(i)+str(14))) 
        f.write(" {},  {},  {},   {},   {}  \n".format('2', str(i)+str(21), str(i)+str(22), str(i)+str(23), str(i)+str(24)))
        f.write(" {},  {},  {},   {},   {}  \n".format('3', str(i)+str(31), str(i)+str(33), str(i)+str(33), str(i)+str(34)))
        f.write(" {},  {},  {},   {},   {}  \n".format('4', str(i)+str(41), str(i)+str(34), str(i)+str(43), str(i)+str(44)))
        

        
class tester(unittest.TestCase):
    def setUp(self):
        #directories
        self.orig_dir=os.getcwd()
        self.temp_dir=tempfile.mkdtemp("_tdir_")
        self.out_dir = tempfile.mkdtemp("_odir_") 
        
        os.chdir(self.temp_dir)
        
        ''' write some files into this dir strucutre
             -- my_sub_dir_1
                 -- TRANS_OD_table_100
                 -- TRANS_OD_table_200
             -- my_sub_dir_2
                 -- TRANS_OD_table_300
                 -- TRANS_OD_table_400   
        '''        

        subdir='my_sub_dir_1'
        os.mkdir(subdir)
        writeFile(os.path.join(self.temp_dir, subdir),100)
        writeFile(os.path.join(self.temp_dir, subdir),200)
        subdir='my_sub_dir_2'
        os.mkdir(subdir)
        writeFile(os.path.join(self.temp_dir, subdir),300)
        writeFile(os.path.join(self.temp_dir, subdir),400)
        
        self.numfiles=4
    
    def tearDown(self):
        os.chdir(self.orig_dir)
        shutil.rmtree(self.temp_dir)
        shutil.rmtree(self.out_dir)
        
    def test_flat_files_partitioned(self)    :
        """make sure the flat files are created correctly, even when 
            forced to be partitioned"""
        
        #This creates consolidated flat files from our test files
        build_flat_files.build_flat_files(self.temp_dir, self.out_dir, test_max_rows=2)

        #gather and sort the output files
        test_files=[]
        for root, dirs, files in os.walk(self.out_dir):
            for f in files:
                test_files.append(os.path.join(root, f))       
        test_files.sort()
        
        for t in test_files:
            """make sure that contents of the output files match what we expect.  So what do we expect?
               
               Directory /my_sub_dir_1/ has two files called TRANS_OD__100 and TRANS_OD__200.
               Each file has a header row, a header column. Other elements are OD pairs (row=O, col =D).
               
               From these files, we are creating a new file with a header row: 
               #header  origin, destination, <file 100 header>, <file 200 header>
               ... designating origin, destination, and vectorized versions of file 100 and file 200 like this:
               
               <origin>  <destination>   <file 100 series><row><col>  <file 200 series><row><col>
               
               The first few rows for my_sub_dir_1.csv would be be:
               1, 1, 10011, 20011
               1, 2, 10012, 20012
               1, 3, 10013, 20013
               
               ... the first data column ends with '100', designating the name of the file TRANS_OD_100 and the second
               begins with '200', designating the name of the file TRANS_OD_table_200
               
            """
            
            with open(t, 'r') as f:
                lines = 0
                content= f.readlines()
                
                #strip the header and clean it up
                header=content.pop(0)
                header=header.replace('#','').strip().split('|')
                
                self.assertEqual(len(content), 8) #rows*cols
               
                #we'll check the first and last rows, along w/ total count
                first_row=content[0]
                last_row=content[-1]
                    
                if 'my_sub_dir_1_data0' in os.path.splitext(os.path.basename(t))[0]:
                    #the 100 and 200 series data (table data elements start with '100' and '200')
                    self.assertEqual(first_row.strip(), '1,1,10011,20011')  #o=1, d=1, (100 series, 11), (200 series, 11)
                    self.assertEqual(last_row.strip(),  '2,4,10024,20024')  #o=4, d=4, (100 series, 11), (200 series, 11)
                if 'my_sub_dir_2_data0' in os.path.splitext(os.path.basename(t))[0]:
                    #the 400 and 300 series data (table data elements  with '300' and '400')
                    self.assertEqual(first_row.strip(), '1,1,30011,40011')  #o=1, d=1, (100 series, 11), (200 series, 11)
                    self.assertEqual(last_row.strip() , '2,4,30024,40024')  #o=4, d=4, (100 series, 11), (200 series, 11) 
                                
        
    def test_flat_files(self)    :
        "make sure the flat files are created correctly"
        
        #This creates consolidated flat files from our test files
        build_flat_files.build_flat_files(self.temp_dir, self.out_dir)

        #gather and sort the output files
        test_files=[]
        for root, dirs, files in os.walk(self.out_dir):
            for f in files:
                test_files.append(os.path.join(root, f))       
        test_files.sort()
        
        for t in test_files:
            """make sure that contents of the output files match what we expect.  So what do we expect?
               
               Directory /my_sub_dir_1/ has two files called TRANS_OD__100 and TRANS_OD__200.
               Each file has a header row, a header column. Other elements are OD pairs (row=O, col =D).
               
               From these files, we are creating a new file with a header row: 
               #header  origin, destination, <file 100 header>, <file 200 header>
               ... designating origin, destination, and vectorized versions of file 100 and file 200 like this:
               
               <origin>  <destination>   <file 100 series><row><col>  <file 200 series><row><col>
               
               The first few rows for my_sub_dir_1.csv would be be:
               1, 1, 10011, 20011
               1, 2, 10012, 20012
               1, 3, 10013, 20013
               
               ... the first data column ends with '100', designating the name of the file TRANS_OD_100 and the second
               begins with '200', designating the name of the file TRANS_OD_table_200
               
            """
            
            with open(t, 'r') as f:
                lines = 0
                content= f.readlines()
                
                #strip the header and clean it up
                header=content.pop(0)
                header=header.replace('#','').strip().split('|')
                
                self.assertEqual(len(content), 16) #rows*cols
               
                #we'll check the first and last rows, along w/ total count
                first_row=content[0]
                last_row=content[-1]
                    
                if os.path.splitext(os.path.basename(t))[0] == 'my_sub_dir_1_data':
                    #self.assertTrue(expr)
                    #the 100 and 200 series data (table data elements start with '100' and '200')
                    self.assertEqual(first_row.strip(), '1,1,10011,20011')  #o=1, d=1, (100 series, 11), (200 series, 11)
                    self.assertEqual(last_row.strip(),  '4,4,10044,20044')  #o=4, d=4, (100 series, 11), (200 series, 11)
                if os.path.splitext(os.path.basename(t))[0] == 'my_sub_dir_2_data':  
                    #the 400 and 300 series data (table data elements  with '300' and '400')
                    self.assertEqual(first_row.strip(), '1,1,30011,40011')  #o=1, d=1, (100 series, 11), (200 series, 11)
                    self.assertEqual(last_row.strip() , '4,4,30044,40044')  #o=4, d=4, (100 series, 11), (200 series, 11) 
                        
    def test_load_with_copy_partitioned(self):
        "ensures data tables are loading correctly by loading these flat files"
        build_flat_files.build_flat_files(self.temp_dir, self.out_dir,test_max_rows=2)
        #loads the data tables (INSERT method)
        fn=os.path.join(self.out_dir, 'my_sub_dir_1_data.csv')
        t_name='dir_1_data'
        table=build_tables.build_tables(db=DB, pdir=self.out_dir, drop_old=True)
        
        #grab the data and ensure it's right
        curs.execute('END')
        curs.execute("SELECT * FROM {}".format(t_name))
        actual=curs.fetchall()
        
        target=[(1, 1, 10011, 20011),
                (1, 2, 10012, 20012),
                (1, 3, 10013, 20013),
                (1, 4, 10014, 20014),
                (2, 1, 10021, 20021),
                (2, 2, 10022, 20022),
                (2, 3, 10023, 20023),
                (2, 4, 10024, 20024),
                (3, 1, 10031, 20031),
                (3, 2, 10033, 20033),
                (3, 3, 10033, 20033),
                (3, 4, 10034, 20034),
                (4, 1, 10041, 20041),
                (4, 2, 10034, 20034),
                (4, 3, 10043, 20043),
                (4, 4, 10044, 20044)]
        
        for t, a in zip(target, actual):
            self.assertEqual(t, a, 'load_with_insert failed'  )
                        
    def test_load_with_copy(self):
        "ensures data tables are loading correctly by loading these flat files"
        build_flat_files.build_flat_files(self.temp_dir, self.out_dir)
        #loads the data tables (INSERT method)
        fn=os.path.join(self.out_dir, 'my_sub_dir_1_data.csv')
        t_name='dir_1_data'
        table=build_tables.build_tables(db=DB, pdir=self.out_dir, drop_old=True)
        
        #grab the data and ensure it's right
        curs.execute('END')
        curs.execute("SELECT * FROM {}".format(t_name))
        actual=curs.fetchall()
        
        target=[(1, 1, 10011, 20011),
                (1, 2, 10012, 20012),
                (1, 3, 10013, 20013),
                (1, 4, 10014, 20014),
                (2, 1, 10021, 20021),
                (2, 2, 10022, 20022),
                (2, 3, 10023, 20023),
                (2, 4, 10024, 20024),
                (3, 1, 10031, 20031),
                (3, 2, 10033, 20033),
                (3, 3, 10033, 20033),
                (3, 4, 10034, 20034),
                (4, 1, 10041, 20041),
                (4, 2, 10034, 20034),
                (4, 3, 10043, 20043),
                (4, 4, 10044, 20044)]
        
        for t, a in zip(target, actual):
            self.assertEqual(t, a, 'load_with_insert failed'  )
    
    def xtest_load_with_insert(self):
        "ensures data tables are loading correctly by loading these flat files"
        build_flat_files.build_flat_files(self.temp_dir, self.out_dir)
        #loads the data tables (INSERT method)
        files = os.listdir(self.out_dir)
        files.sort()
        first_file = files[0]
        fn = os.path.join(self.out_dir,first_file) 
        t_name = utils_and_settings.get_table_name_from_fn(first_file)
        table=build_tables.load_with_insert(db=DB, t_name=t_name, file=fn, drop_old=True)

        #grab the data and ensure it's right
        curs.execute('END')
        curs.execute("SELECT * FROM {}".format(t_name))
        actual=curs.fetchall()
        
        target=[(1, 1, 10011, 20011),
                (1, 2, 10012, 20012),
                (1, 3, 10013, 20013),
                (1, 4, 10014, 20014),
                (2, 1, 10021, 20021),
                (2, 2, 10022, 20022),
                (2, 3, 10023, 20023),
                (2, 4, 10024, 20024),
                (3, 1, 10031, 20031),
                (3, 2, 10033, 20033),
                (3, 3, 10033, 20033),
                (3, 4, 10034, 20034),
                (4, 1, 10041, 20041),
                (4, 2, 10034, 20034),
                (4, 3, 10043, 20043),
                (4, 4, 10044, 20044)]
        
        for t, a in zip(target, actual):
            self.assertEqual(t, a, 'load_with_insert failed'  )            
        
if __name__=='__main__':
    unittest.main()
        
    

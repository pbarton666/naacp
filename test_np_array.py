import unittest
import psycopg2
from  database import login_info
import tempfile
import os
import numpy as np
import build_flat
from pprint import pprint
import shutil
import logging
import logging.handlers

from settings import DB, FN_DELIMITER

conn = psycopg2.connect(database= DB,
    user=login_info['user'],
    password=login_info['password']
    )
curs = conn.cursor() 

logger = logging.getLogger('trans_logger')

def writeFile(subdir, i):
    #creates a predictable test data file""
    fn='some_great_junk'
    file=fn + str(i)
    with open(os.path.join(subdir,file), 'w') as f:
        f.write(" {},  {},  {},  {},    {}   \n".format('0',    ' 1',     '2',     '3',      '4' ))
        f.write(" {},  {},  {},   {},   {}  \n".format('1', str(i)+str(11), str(i)+str(12), str(i)+str(13), str(i)+str(14))) 
        f.write(" {},  {},  {},   {},   {}  \n".format('1', str(i)+str(21), str(i)+str(22), str(i)+str(23), str(i)+str(24)))
        f.write(" {},  {},  {},   {},   {}  \n".format('1', str(i)+str(31), str(i)+str(33), str(i)+str(33), str(i)+str(34)))
        f.write(" {},  {},  {},   {},   {}  \n".format('1', str(i)+str(41), str(i)+str(34), str(i)+str(43), str(i)+str(44)))
        
class tester(unittest.TestCase):
    def setUp(self):
        #temp dirs
        self.orig_dir=os.getcwd()
        self.temp_dir=tempfile.mkdtemp()
        os.chdir(self.temp_dir)

        self.out_dir = tempfile.mkdtemp()       
        
        #write some files
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
        
    def testNumpy(self)    :
        "make sure the file imports/exports are working correctly"
        #sic it on the parent directory of the fake files - will write file
        build_flat.create_np_array(self.temp_dir, self.out_dir)

        #fake files in different subs (the data will differ by ix)
        
        test_files=[]
        for root, dirs, files in os.walk(self.out_dir):
            for f in files:
                test_files.append(os.path.join(root, f))

        
        test_files.sort()
        #these are now sorted: dir1 columns, dir 1 data, dir2 columns, dir 2 data
        for t in test_files:
            #this is a data file; check total cols, lines and values of first and last
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
                
                #the header is a list something like:  ['great_junk100', 'great_junk200']
                ##TODO:  the col order is a function of os file name sorting 
                for h in header:
                    if '100' in h:  #the 100 and 200 series data
                        self.assertEqual(first_row.strip(), '1,1,10011,20011')  #o=1, d=1, (100 series, 11), (200 series, 11)
                        self.assertEqual(last_row.strip(),  '4,4,10044,20044')  #o=4, d=4, (100 series, 11), (200 series, 11)
                    if '300' in h:  #the 400 and 300 series data
                        self.assertEqual(first_row.strip(), '1,1,40011,30011')  #o=1, d=1, (100 series, 11), (200 series, 11)
                        self.assertEqual(last_row.strip() , '4,4,40044,30044')  #o=4, d=4, (100 series, 11), (200 series, 11)                                        
        
if __name__=='__main__':
    unittest.main()
        
    

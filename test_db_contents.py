"""Tests that all the data tables have been loaded.  """
import unittest
import psycopg2
import csv
import os
from pprint import pprint as pp
import numpy as np

import utils_and_settings
from database import login_info
import utils_and_settings

#the same one used in build_all
dirs = [{'data': '/home/pat/mary/NoRed-YesHwy',  'scratch': '/home/pat/mary/flat_NoRed-YesHwy'},
        {'data': '/home/pat/mary/NoRed-NoHwy',   'scratch': '/home/pat/mary/flat_NoRed-NoHwy'},
        {'data': '/home/pat/mary/YesRed-YesHwy', 'scratch': '/home/pat/mary/flat_YesRed-YesHwy'},
        {'data': '/home/pat/mary/YesRed-NoHwy',  'scratch': '/home/pat/mary/flat_YesRed-NoHwy'},
        # {'data': '/home/pat/mary/Test-NoRed-YesHwy',  'scratch': '/home/pat/mary/junkdir'},
        ]
DEBUG=True
DB='naacp'
EXP_ROWS=1390041 #expected rows
LAST_ORIG=1179
LAST_DEST=1179

conn = psycopg2.connect(database= DB,
    user=login_info['user'],
    password=login_info['password']
    )
curs = conn.cursor() 

def find_table_data(data_dirs):
    "finds all the tables the db is supposed to have, along with"
    data_dirs=data_dirs
    table_data={} #keys: table names, values: col_data dicts
    #Go thru the data directories.  Build a mapping between them and the db tables
    for in_dir in data_dirs:  #'dirs' is the main dict "scenario level" directories
        for root, dirs, files in os.walk(in_dir['data']):
            
            tnames=[]          #just the table names
            tname_col_file=[]  #tuples of table name, column name, source file name            
            #each of these directories populates a db table
            
            for d in dirs:   
                #the table is named after the dir
                raw_name= utils_and_settings.get_table_name_from_dir(os.path.join(root,d))
                t_name=raw_name.replace('-','_')
                tnames.append(t_name)
                
                col_data=[]
                
                #each column is named after a file
                for f in os.listdir(os.path.join(root, d)):
                    fn = os.path.join(root, d, f) 
                    col_data.append({'scenario_dir': in_dir,
                                     'file' : os.path.join(in_dir, root, d, f),
                                     'col': utils_and_settings.get_column_name(os.path.join( root, d, f))
                                     }
                                    )
                
                table_data[t_name]=col_data
    return(table_data)   

def find_db_tables():
    "Gets the tables from the db and the associated source files.  Returns dict"
    #all the table names
    curs.execute("SELECT table_name FROM information_schema.tables WHERE table_schema = 'public';")
    t_names = curs.fetchall()  
    tables=[]
    for t in t_names:
        tables.append(t[0])
    return tables
                     
class test(unittest.TestCase):
    "Test the the data tables created match the expected values."
    def tests(self):
        "verify data table contents"
        #The table names encode scenario and directory represented.  So the table_data dict has a record
        #  of all the tables we're supposed to have, all the column info, and the source data
        
        #This logic interrogates the table to ensure the contents are right, it's the right size, etc.
        
        tables=find_db_tables()
        table_data=find_table_data(dirs)
        missing_tables=[]
        
        for t_name in table_data.keys():
            if t_name in tables:
                #right number of rows?
                curs.execute("SELECT COUNT(*) FROM {}".format(t_name))
                res=curs.fetchone()
                count=res[0]
                self.assertEqual(count, EXP_ROWS)
                    
                #right number of column values?
                sql="SELECT MAX(dest) FROM {} WHERE origin=1"
                curs.execute(sql.format(t_name))
                res=curs.fetchone()
                val=res[0]
                self.assertEqual(val, LAST_DEST)                
                
                #we'll check the next-to-last row for each column. If it's right, the rest probably is.
                #  (the last row and column are zeros, accidentally or otherwise)
                col_data=table_data[t_name]
                for col in col_data:    
                    source_file = col['file']
                    db_col = col['col']
                    
                    #the db value
                    sql = 'SELECT {} FROM {} WHERE origin = {} AND dest = {}'
                    curs.execute(sql.format(db_col, t_name, LAST_ORIG-1, LAST_DEST-1))
                    res= curs.fetchone()
                    db_val=res[0]
                    
                    #the file value should be the last element of the last row
                    source=np.genfromtxt(source_file, delimiter=',', dtype=int)
                    source_val=source[-2][-2]                    
                    self.assertEqual(db_val,source_val )

                    
            else:
                missing_tables.append(t_name)
                    
            self.assertTrue(len(missing_tables)==0, 'missing from db: {}'.format(missing_tables))
        
if __name__=='__main__':
    unittest.main()
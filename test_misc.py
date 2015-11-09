
"""miscellaneous tests"""
import unittest
import utils_and_settings

class test_me(unittest.TestCase):
    def test_names(self):
        "makes sure column names appear as expected"
        test='/home/pat/data/RL_Analysis/NoRed-Hwy/HWY_OD_TimeCost/HWY_OP_HOV_DISTANCE.csv'
        table=utils_and_settings.get_table_name_from_dir(test)
        self.assertEqual(table, 'hwy_od_timecost_hwy_op_hov_distance.csv')
        col = utils_and_settings.get_column_name('HWY_OP_HOV_DISTANCE.csv')        
        self.assertEqual(col, 'op_hov_distance')
        x=1
        
    def test_make_OD_array(self):
        """ creates vectors to represent origins and destinations."""
        
        #make_OD_array(rows, cols=None, start_row=1, max_row=99999):
        #3 rows x 3 cols
        result=utils_and_settings.make_OD_array(rows=3, cols=None, start_row=1)
        target=[[1, 1, 1, 2, 2, 2, 3, 3, 3], #origin vector
                [1, 2, 3, 1, 2, 3, 1, 2, 3]] #destination vector
        self.assertEqual(result,target)
        
        #2 rows x 4 cols
        result=utils_and_settings.make_OD_array(rows=2, cols=4, start_row=1)
        target=[[1, 1, 1, 1, 2, 2, 2, 2], 
                [1, 2, 3, 4, 1, 2, 3, 4]]
        self.assertEqual(result,target)
        
        #2 rows x 4 cols, starting at 500
        result=utils_and_settings.make_OD_array(rows=2, cols=4, start_row=500)
        target=[[500, 500, 500, 500, 501, 501, 501, 501], 
                [1, 2, 3, 4, 1, 2, 3, 4]]
        self.assertEqual(result,target)  
        
        #2 rows x 4 cols, starting at 500
        result=utils_and_settings.make_OD_array(rows=3, cols=4, start_row=500, max_row=501)
        target=[[500, 500, 500, 500, 501, 501, 501, 501], 
                [1, 2, 3, 4, 1, 2, 3, 4]]
        self.assertEqual(result,target)
        
        #real-world - 500 rows, 1179 columns, start2
        result=utils_and_settings.make_OD_array(rows=500, cols=1179, start_row=2, max_row=1179)    
        self.assertEqual(result[0][-1],501)
        self.assertEqual(result[1][-1],1179)
        
        #real-world - 500 rows, 1179 columns, start2
        result=utils_and_settings.make_OD_array(rows=500, cols=1179, start_row=500, max_row=1179)  
        last_row = result[0][-1]
        first_row=result[0][0]
        last_col = result[1][-1]
        first_col=result[1][0]
        self.assertEqual(first_row, 500)
        self.assertEqual( last_row,999)
        self.assertEqual(first_col, 1)
        self.assertEqual(last_col,1179) 
        
        #real-world - 500 rows, 1179 columns, start2
        result=utils_and_settings.make_OD_array(rows=500, cols=1179, start_row=1001, max_row=1179)    
        last_row = result[0][-1]
        first_row=result[0][0]
        last_col = result[1][-1]
        first_col=result[1][0]
        self.assertEqual(first_row, 1001)
        self.assertEqual( last_row,1179)
        self.assertEqual(first_col, 1)
        self.assertEqual(last_col,1179)       
        

    def test_get_window(self):
        init_rows = 1179#1179 
        rows_per = 500 
        start= 1
        
        #init_rows = 8 
        #rows_per = 3 
        #start= 2        
        ret = utils_and_settings.get_window(init_rows, rows_per, start)

        d=next(ret)
        self.assertEqual(d['first_row'], 1)
        self.assertEqual(d['last_row'], 500)
        self.assertEqual(d['skip_header'], 0)
        self.assertEqual(d['skip_footer'], 679)
        d=next(ret)
        self.assertEqual(d['first_row'], 501)
        self.assertEqual(d['last_row'], 1000)
        self.assertEqual(d['skip_header'], 500)
        self.assertEqual(d['skip_footer'], 179)        
        d=next(ret)
        self.assertEqual(d['first_row'], 1001)
        self.assertEqual(d['last_row'], 1179)
        self.assertEqual(d['skip_header'], 1000)
        self.assertEqual(d['skip_footer'], 0)        
         
    
if __name__=='__main__':
    unittest.main()

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
        #3 rows x 3 cols
        result=utils_and_settings.make_OD_array(3, cols=None, start_row=1)
        target=[[1, 1, 1, 2, 2, 2, 3, 3, 3], #origin vector
                [1, 2, 3, 1, 2, 3, 1, 2, 3]] #destination vector
        self.assertEqual(result,target)
        
        #2 rows x 4 cols
        result=utils_and_settings.make_OD_array(2, cols=4, start_row=1)
        target=[[1, 1, 1, 1, 2, 2, 2, 2], 
                [1, 2, 3, 4, 1, 2, 3, 4]]
        self.assertEqual(result,target)

    def test_get_window(self):
        init_rows = 5 #100
        rows_per = 2 #210
        start= 2
        ret = utils_and_settings.get_window(init_rows, rows_per, start)

        d=next(ret)
        self.assertEqual(d['skip_header'], 1)
        self.assertEqual(d['skip_footer'], 2)
        self.assertEqual(init_rows - d['skip_footer'] - d['skip_header'], rows_per)
        d=next(ret)
        self.assertEqual(d['skip_header'], 3)
        self.assertEqual(d['skip_footer'], 0)
        self.assertEqual(init_rows - d['skip_footer'] - d['skip_header'], rows_per)            
            
        
    
if __name__=='__main__':
    unittest.main()

"""miscellaneous tests"""
import unittest
import utils_and_settings

class test_me(unittest.TestCase):
    def test_names(self):
        "makes sure column names appear as expected"
        test='/home/pat/data/RL_Analysis/NoRed-Hwy/HWY_OD_TimeCost/HWY_OP_HOV_DISTANCE.csv'
        table=utils_and_settings.get_table_name_from_dir(test)
        col = utils_and_settings.get_column_name('HWY_OP_HOV_DISTANCE.csv')
        self.assertEqual(table, 'nored-hwy_hwy_od')
        self.assertEqual(col, 'op_hov_distance')
        x=1
        
    






if __name__=='__main__':
    unittest.main()
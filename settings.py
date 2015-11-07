"""main settings for db import project"""
PARENT_DIR = '/home/pat/data/raw_data_unziped'
OUT_DIR = '/home/pat/data/raw_data_unziped'
DB = 'naacp'
FN_DELIMITER = '_'  #info about matrices is stored in the file names themselves.

dir_map = {'HWY_OD_TimeCost':                    'hwy_od',
           'ModeChoice_OD_byPurposeIncomeMode':  'mode_choice_od',
           'PersonTrip_OD_byPurposeIncome':      'person_trip_od',
           'TOD_OD_byPurposeIncomeTODOccupancy': 'tod_od',
           'Transit_OD_TimeCost':                'transit_od'
           }

def get_column_name(file):
    "turn a file name into a db column name"
    #example:  TOD_NHBW_MD_HOV3 -> nhbw_md_hov3
    #file name w/o ext
    raw_name=os.path.splitext(os.path.basename(file))[0]
    #get rid of the bit before the first '_' and make lower case
    col_name=FN_DELIMITER.join(raw_name.split(FN_DELIMITER)[1:]).lower()
    return col_name

def get_table_name(dname):
    """Does not quite work as Carl would like.  Psql makes everything lc, anyway."""

    """Finds the database table name associated with a directory name.
       If the name has not been mapped in dir_map, a rule-based name is
       applied. The first two bits are kept, prepeneded if first char is UC.

       THERE_IS_ALWAYS_NEXT_YEAR -> there_is
       gO_CUBS_2016 -> go_cubs
       """

    t_name = dir_map.get(dname, 0)
    #if no mapping, make up a name
    if not t_name:
        bits = dname.split('_')
        t_name=bits.join(bits[2:])
    return t_name.lower()


#a cursor to use at leisure
#conn = psycopg2.connect(database= self.db,
                             #user=login_info['user'],
                             #password=login_info['password']
                             #)
#curs = self.conn.cursor() 


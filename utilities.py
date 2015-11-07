import os
from settings import FN_DELIMITER


dir_map = {'HWY_OD_TimeCost':                    'hwy_od',
           'ModeChoice_OD_byPurposeIncomeMode':  'mode_choice_od',
           'PersonTrip_OD_byPurposeIncome':      'person_trip_od',
           'TOD_OD_byPurposeIncomeTODOccupancy': 'tod_od',
           'Transit_OD_TimeCost':                'transit_od'
           }

def name_to_flat(fn)        :
    "applies naming convention to flat file"
    return fn[:-4]+'_flat.csv'

def get_column_name(file):
    "turn a file name into a db column name"
    #example:  TOD_NHBW_MD_HOV3 -> nhbw_md_hov3
    #file name w/o ext
    raw_name=os.path.splitext(os.path.basename(file))[0]
    #get rid of the bit before the first '_' and make lower case
    bits = raw_name.split(FN_DELIMITER)
    if len(bits)>=3:
        col_name=FN_DELIMITER.join(raw_name.split(FN_DELIMITER)[1:]).lower()
    elif len(bits) ==2:
        col_name=bits[1]
    else:
        col_name = raw_name
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
        t_name='_'.join(bits[2:])
    return t_name.lower()


def make_OD_array(rows):
    """ orig/dest pairs to match cell contents.  There's probably a
          slicker way to do it in Numpy."""
    arr=[]
    rwa=[]
    ca=[]
    vals = list(range(1, rows+1))
    for r in range(1,rows+1):
        rwa.extend([r]*rows)
        for c in range(1, rows+1):
            ca.extend([c])
    return [rwa,ca]


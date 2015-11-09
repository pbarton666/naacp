import os


# This maps {<name of directory on fs>: <name of db table>}
dir_map = {'HWY_OD_TimeCost':                    'hwy_od',
           'ModeChoice_OD_byPurposeIncomeMode':  'mode_choice_od',
           'PersonTrip_OD_byPurposeIncome':      'person_trip_od',
           'TOD_OD_byPurposeIncomeTODOccupancy': 'tod_od',
           'Transit_OD_TimeCost':                'transit_od'
           }

FN_DELIMITER = '_'  #info about matrices is stored in the file names themselves.

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

def get_table_name_from_dir(dname):
    """Does not quite work as Carl would like.  Psql makes everything lc, anyway."""

    """Finds the database table name associated with a directory name.
       If the name has not been mapped in dir_map, a rule-based name is
       applied. The first two bits are kept, prepeneded if first char is UC.

       THERE_IS_ALWAYS_NEXT_YEAR -> there_is
       gO_CUBS_2016 -> go_cubs
       """

    #grab a short version from the mapping dict
    split_dir = dname.split(os.path.sep)
    t_name = dir_map.get(split_dir[-1], 0) or split_dir[-1]
    
    #prepend the root dir name (these are scenarios)
    if t_name:
        t_name=split_dir[-2] + '_' + t_name 

    return t_name.lower()

def get_table_name_from_fn(fn):
    "trim the _dataXXX.csv bit from the end of the flat file name"
    return fn[:fn.find('_data')]
    


def make_OD_array(rows, cols=None, start_row=1):
    """ orig/dest pairs to match cell contents.  There's probably a
          slicker way to do it in Numpy."""
    arr=[]
    rwa=[]
    ca=[]
    #we'll assume a square array unless we're doing a partition
    if not cols:
        cols = rows
    #vals = list(range(start_row, rows+1))
    for r in range(1,rows+1):
        rwa.extend([r]*cols)
        for c in range(1, cols+1):
            ca.extend([c])
    return [rwa,ca]

def get_window(init_rows, rows_per=None, start=None):
    header_skip = start-1
    footer_skip = init_rows - (start + rows_per) +1
    rows_so_far=0
    while True:
        d={'skip_header': header_skip, 'skip_footer': footer_skip}
        header_skip+=rows_per
        footer_skip-=rows_per
        yield d
        
        yield {'skip_header': header_skip, 'skip_footer': footer_skip}
        
def get_base_fn(fn)     :
    "gets base file name from a partitioned data file"
    #in a file like this: 'tmpxh9vby_tdir__my_sub_dir_2_data2.csv'
    #  the '2' in the 'data2' bit means it's one piece of a partitioned
    #  large directory.  It has a few rows and all of the columns of a
    #  file associated with a large directory.
    #
    return fn[:fn.find('_data')]

        
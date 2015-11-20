import os


# This maps {<name of directory on fs>: <name of db table>}
dir_map = {'HWY_OD_TimeCost':                    'hwy_od',
           'ModeChoice_OD_byPurposeIncomeMode':  'mode_choice_od',
           'PersonTrip_OD_byPurposeIncome':      'person_trip_od',
           'TOD_OD_byPurposeIncomeTODOccupancy': 'tod_od',
           'Transit_OD_TimeCost':                'transit_od',
           #newy addded
           'TOD_OD_byPurposeIncomeTODOccupancy': 'trips_purpose_income_tod_occ'
           }

FN_DELIMITER = '_'  #info about matrices is stored in the file names themselves.

integer_dirs= ['HWY_OD_TimeCost', 'ModeChoice_OD_byPurposeIncomeMode', 
               'PersonTrip_OD_byPurposeIncome', 'TOD_OD_byPurposeIncomeTODOccupancy', 
               'TRANSIT_OD_TimeCost']


def make_names(dirs):

    source_dirs = dirs
    scenarios=set()
    table_root=set()
    tables=set()
    for data_dir in source_dirs:  #'dirs' is the main dict "scenario level" directories
        for root, dirs, files in os.walk(data_dir['data']):
            scenario_name=root.split(os.path.sep)[-1]
            for d in dirs:
                
                short_dir_name = os.path.join(scenario_name, d)
                table_name = get_table_name_from_dir(short_dir_name)
                table_root_name='_'.join(table_name.split('_')[1:])
                scenarios.add(table_name.split('_')[0])
                tables.add(table_name)
                table_root.add(table_root_name)   
    return (scenarios, table_root, tables)

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
    return fn[:fn.find('_data')].replace('-', '_')
    


def make_OD_array(rows=None, cols=None, start_row=1, max_row=99999):
    """ orig/dest pairs to match cell contents.  There's probably a
          slicker way to do it in Numpy."""
    arr=[]
    rwa=[]
    ca=[]
    #we'll assume a square array unless we're doing a partition
    if not cols:
        cols = rows

    for r in range(start_row, min(start_row+rows, max_row+1)):
        rwa.extend([r]*cols)
        for c in range(1, cols+1):
            ca.extend([c])
    return [rwa,ca]

def get_window(init_rows, rows_per=None, start=None, max_rows=1179):
    header_skip = start-1
    footer_skip = init_rows -(header_skip + rows_per ) 
    start=header_skip + 1
    end=init_rows + start-1 - footer_skip 
    #print(start, end)
    rows_so_far=0
    while True:
        d={'skip_header': header_skip, 'skip_footer': footer_skip,
           'first_row': start, 'last_row': end} 
        rows= init_rows - header_skip  - footer_skip
        #print(rows)
        yield d
        header_skip+=rows_per
        footer_skip=max(0, footer_skip-rows_per)         
        start=header_skip+1
        end = init_rows-footer_skip
        
        
def get_base_fn(fn)     :
    "gets base file name from a partitioned data file"
    #in a file like this: 'tmpxh9vby_tdir__my_sub_dir_2_data2.csv'
    #  the '2' in the 'data2' bit means it's one piece of a partitioned
    #  large directory.  It has a few rows and all of the columns of a
    #  file associated with a large directory.
    #
    return fn[:fn.find('_data')]

#w = get_window(1179, 4200,1)
#next(w)
#x=1

        
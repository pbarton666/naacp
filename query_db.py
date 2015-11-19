
import psycopg2
import csv
import os
from pprint import pprint as pp
import numpy as np
#
from logger_setup import logger
from database import login_info

if True:  # constants, globals used for source code folding only
    
    #used to initialize np array
    NP_ROWS=1179  * 1179   #origin, dest pairs
    NP_COLS=10                     #aggregate columns required for each income group
    
    #set to 'WARN' to capure only data loading issues.  'DEBUG' is verbose.
    LOG_LEVEL='INFO'   
    
    #switches to limit queries for testing
    LIMIT_ON = 10                        #arbitrary, small >=1
    LIMIT_OFF= 100000000         #arbitrary, large
    LIMIT_STG =  ' LIMIT {}'      
    
    TEST=True
    if TEST:
        NP_ROWS=4  #conforms to 'test' database
        db='test'
        prefix='test_'  #used only for testing don't want to drop 'real' tables ;-)  
    else:
        prefix=''
    
    
    conn = psycopg2.connect(database= db,
                            user=login_info['user'],
                            password=login_info['password']
                            )
    curs = conn.cursor() 

if True:  #column and table name components (stiched together to id columns)
    scenarios=['yesred_nohwy', 'yesred_yeshwy', 'nored_nohwy', 'nored_nohwy']
    incomes=['inc1', 'inc2', 'inc3', 'inc4', 'inc5']
    rail_modes=['wrail', 'wcrail', 'drail', 'dcrail']
    bus_modes=['wexpbus', 'wbus', 'dexpbus', 'dbus']
    drive_modes=['da', 'sr2', 'sr3']
    purposes_pk=['hbw', 'nhbw']
    purposes_op=['obo', 'hbs', 'hbo']
    purposes = purposes_pk + purposes_op
    
    #Some trip purposes inherently involve a round trip, others one way.
    #   Home-based trips are assumed round-trip
    purposes_round_trip=['hbw', 'hbo', 'hbs']
    purposes_one_way=['obo', 'nhbw']
    
    times=['xferwaittime', 'walktime', 'railtime', 'initialwaittime', 'bustime', 'autotime']
    placeholders=['fare', 'autodistance']  #used as placeholders
    occupancy_hwy_loaded=['hov', 'sov']
    occupancy_hwy_tod=['sov', 'hov2', 'hov3']
    tod_hwy_loaded=['am', 'md','pm', 'nt']
    tod_transit_times=['pk', 'op']
    tod_fares=tod_transit_times
    metrics=['toll', 'time', 'distance']    
    
    #mappings
    purpose_peak_flag={}  #mapped to abbreviations in tables
    for p in purposes_pk:
        purpose_peak_flag[p] = 'pk'
    for p in purposes_op:
        purpose_peak_flag[p] = 'op'
        
    purpose_rount_trip_flag=purpose_peak_flag
    
def aggregate_bus_rail_fares(scenario=None,   
                                                    income=None,  
                                                    purposes=purposes,      
                                                    purposes_round_trip=purposes_round_trip,
                                                    #purpose_peak_flag=purpose_peak_flag, 
                                                    bus_modes=bus_modes, 
                                                    rail_modes=rail_modes,
                                                    np_rows=NP_ROWS):
                                                    
    
    " rail/bus costs = SUM((peak trips * peak fare) + (op trips * op fare))"
    
    """
    The main assumption is that work-related trips are on peak, and others are off.  Here are the mappings:

    purposes_pk=[('hbw', 1), ('nhbw', 2)]
    purposes_op=[('obo', 3), ('hbs', 4), ('hbo', 5)]

    The mode_choice_od table disaggregates to income, mode, purpose.  For instance, we have data for
    trips from home to work, walking to the express bus, on-peak for each OD pair.  From the fares_fares table,
    we know the fare for the express bus on-peak.   

    So we can simply aggregate across all the mode choice/purpose pairs to capture all trips associated with some
    income group and assign the correct fair to each.

    We do this by coming up with a bunch of SELECT statements like this one:

    '--adding peak rail/bus fare costs
       SELECT  DISTINCT
          test_yesred_nohwy_mode_choice_od.origin.
          test_yesred_nohwy_mode_choice_od.dest,
          test_yesred_nohwy_mode_choice_od.hbw_inc1_wexpbus * test_yesred_nohwy_mode_choice_od.wexpbus_pk
      FROM 
         test_yesred_nohwy_test_yesred_nohwy_mode_choice_od , test_fares_fares.wexpbus._pk  
      WHERE  
         test_yesred_nohwy_mode_choice_od.origin=test_fares_fares.origin AND 
         test_yesred_nohwy_mode_choice_od.dest=test_fares_fares.dest 
      ORDER BY 
         test_yesred_nohwy_.origin, test_yesred_nohwy_mode_choice_od.dest
        """          
    #general info for this metric
    topic = 'bus_rail_fares'    
    trips_table = '{}mode_choice_od'.format(scenario)
    metrics_table= '{}fares_fares'.format(prefix)
    logger.info('Beginning aggregation of {} data'.format(topic))
    
    #initialize null np array
    npa=np.zeros((np_rows, 3))  #orig, dest, value
    fresh_npa_array=True
    
    for purpose in purposes:
        #peak or off peak as f(purpose) 
        pk_flag=purpose_peak_flag[purpose]
        
        #round trip of one-way (round trip for home based journeys)?
        trip_legs=['outbound']
        if purpose in purposes_round_trip:
            trip_legs.append('return')
          
        #calculate each leg of the trip separately  
        for trip_leg in trip_legs:      
            
            #loop thru appropriate modes (no fares w/ auto) and compose SELECT
            for mode in bus_modes+rail_modes:
                    #          --adding {topic} for {purpose}. {mode}\n' 
                    select= '--adding {} for {}. {}\n'.format(topic, purpose, mode)
                    #          SELECT DISTINCT
                    select += 'SELECT  DISTINCT\n '
                    #                 {metrics_table}.origin
                    select += '\t{}.origin,\n'.format(metrics_table)
                    #                 {metrics_table}.dest            
                    select += '\t{}.dest,\n'.format(metrics_table)   
                    #               '{trips_table}.{purpose}_{income}_{mode} * {fares_table}.{mode}_{pk_flag} '
                    stmt=       '\t{}.{}_{}_{} * {}.{}_{}\n '
                    select+= stmt.format(trips_table, purpose, income, mode, metrics_table, mode, pk_flag)
                    #                FROM {trips_table} , {metrics_table}
                    select += 'FROM \n\t {} , {} \n '.format( trips_table, metrics_table)
                    
                    if trip_leg== 'outbound':   
                        #use OD pairs from trip table same as metric table's
    
                        #               WHERE  {trips_table}.origin={metrics_table}.origin AND 
                        select +='WHERE  \n\t{}.origin={}.origin AND \n'.format( trips_table, metrics_table)
                        #                   {metrics_table}.dest={metrics_table}.dest)
                        select +='\t{}.dest={}.dest \n'.format(metrics_table, trips_table)
                        
                    else:  
                        #use transposed OD pairs from trip table (origin = metrics.dest, dest=metrics.origin)
                        
                        #               WHERE  {trips_table}.dest={metrics_table}.origin AND 
                        select +='WHERE  \n\t{}.dest={}.origin AND \n'.format( trips_table, metrics_table)
                        #                   {metrics_table}.origin={metrics_table}.dest)
                        select +='\t{}.origin={}.dest \n'.format(trips_table, metrics_table)                        
                        
                    #             ORDER BY {metrics_table}.origin, {metrics_table}.dest
                    select +='ORDER BY \n\t{}.origin, {}.dest\n\n'.format( metrics_table, metrics_table)   
                    #print(select)
                    curs.execute(select)
                    res=np.array(curs.fetchall())     
                    
                    
                    #add the results to the last column of the aggregation array
                    #grab the OD columns for the first result
                    if fresh_npa_array:    
                        npa=res
                        fresh_npa_array=False
                    #add only the last column of subsequent ones
                    else:
                        npa[:,-1]+=res[:,-1]
                 
                    logger.info('writing data for {} {} for {}: {} {}  {}'.format(pk_flag, topic, scenario, income, purpose, mode))
                    logger.debug('executing {}\n'.format(select))
    return(npa)


def aggregate_transit_times(scenario=None,   
                                                    income=None,  
                                                    purposes=purposes,      
                                                    purposes_round_trip=purposes_round_trip,
                                                    bus_modes=bus_modes, 
                                                    rail_modes=rail_modes,
                                                    np_rows=NP_ROWS,
                                                    topic=None):
    
    "Aggregate time costs for mass transit.  Cf aggregate_bus_rail_fares() for more verbose documentation."
                                                    
    
    " rail/bus costs = SUM((peak trips * peak fare) + (op trips * op fare))"
             
    #general info for this metric
    trips_table = '{}mode_choice_od'.format(scenario)
    metrics_table= '{}fares_fares'.format(prefix)
    logger.info('Beginning aggregation of {} data'.format(topic))
    
    #initialize null np array
    npa=np.zeros((np_rows, 3))  #orig, dest, value
    fresh_npa_array=True
    
    for purpose in purposes:
        #peak or off peak as f(purpose) 
        pk_flag=purpose_peak_flag[purpose]
        
        #round trip of one-way (round trip for home based journeys)?
        trip_legs=['outbound']
        if purpose in purposes_round_trip:
            trip_legs.append('return')
          
        #calculate each leg of the trip separately  
        for trip_leg in trip_legs:      
            
            #loop thru appropriate modes (no fares w/ auto) and compose SELECT
            for mode in bus_modes+rail_modes:
                    #          --adding {topic} for {purpose}. {mode}\n' 
                    select= '--adding {} for {}. {}\n'.format(topic, purpose, mode)
                    #          SELECT DISTINCT
                    select += 'SELECT  DISTINCT\n '
                    #                 {metrics_table}.origin
                    select += '\t{}.origin,\n'.format(metrics_table)
                    #                 {metrics_table}.dest            
                    select += '\t{}.dest,\n'.format(metrics_table)   
                    #               '{trips_table}.{purpose}_{income}_{mode} * {fares_table}.{mode}_{pk_flag} '
                    stmt=       '\t{}.{}_{}_{} * {}.{}_{}\n '
                    select+= stmt.format(trips_table, purpose, income, mode, metrics_table, mode, pk_flag)
                    #                FROM {trips_table} , {metrics_table}
                    select += 'FROM \n\t {} , {} \n '.format( trips_table, metrics_table)
                    
                    if trip_leg== 'outbound':   
                        #use OD pairs from trip table same as metric table's
    
                        #               WHERE  {trips_table}.origin={metrics_table}.origin AND 
                        select +='WHERE  \n\t{}.origin={}.origin AND \n'.format( trips_table, metrics_table)
                        #                   {metrics_table}.dest={metrics_table}.dest)
                        select +='\t{}.dest={}.dest \n'.format(metrics_table, trips_table)
                        
                    else:  
                        #use transposed OD pairs from trip table (origin = metrics.dest, dest=metrics.origin)
                        
                        #               WHERE  {trips_table}.dest={metrics_table}.origin AND 
                        select +='WHERE  \n\t{}.dest={}.origin AND \n'.format( trips_table, metrics_table)
                        #                   {metrics_table}.origin={metrics_table}.dest)
                        select +='\t{}.origin={}.dest \n'.format(trips_table, metrics_table)                        
                        
                    #             ORDER BY {metrics_table}.origin, {metrics_table}.dest
                    select +='ORDER BY \n\t{}.origin, {}.dest\n\n'.format( metrics_table, metrics_table)   
                    #print(select)
                    curs.execute(select)
                    res=np.array(curs.fetchall())     
                    
                    
                    #add the results to the last column of the aggregation array
                    #grab the OD columns for the first result
                    if fresh_npa_array:    
                        npa=res
                        fresh_npa_array=False
                    #add only the last column of subsequent ones
                    else:
                        npa[:,-1]+=res[:,-1]
                 
                    logger.info('writing data for {} {} for {}: {} {}  {}'.format(pk_flag, topic, scenario, income, purpose, mode))
                    logger.debug('executing {}\n'.format(select))
    return(npa)

def aggregate_hwy_costs(scenario=None,income=None)    :
    pass
    
    
def add_to_master(npa=None, master_npa=None, master_col=None, include_od_cols=False):
    "adds newly-acquired data column to the master array"
    if include_od_cols:
        master_npa=npa
    else:
        master_npa[:,mater_col]+=npa[:,-1]
    return master_npa

def make_rollup_tables():

    for scenario in   scenarios:
        scenario=prefix + scenario+'_'
        
        for income in incomes:
            
            #initialize np array
            master_npa=np.zeros((NP_COLS, NP_COLS)) 
            master_col =0 #numpy column index
            master_headers=['origin', 'dest']
            
            #add orig, dest columns to master_npa, since this is the first one loaded
            col_name = 'bus_rail_fares'                 #column name in master_npa array
            routine=aggregate_bus_rail_fares      #the method run to process this
            
             #add orig, dest columns to master_npa, since this is the first one loaded
            master_npa = add_to_master( master_npa=master_npa,
                                                             npa=routine(scenario=scenario, income=income), 
                                                             master_col=master_col, 
                                                             include_od_cols=True)
            master_headers.append(col_name)
            master_col +=1
            logger.info('done rolling up {} {} {}'.format(scenario, income, col_name))
            
            
            
            #highway distances, tolls, time
            #do this separately !!!
            master_npa = add_to_master(load_hwy_costs(income), np_col)
            master_headers.append('bus_rail_fares')
            master_col +=1
            logger.info('done rolling up transit costs for {} {}'.format(scenario, income))            
               
            #times - gotta do redo w/o aggretagions
            #load_transit_times()
            
            #make db table for master


if __name__=='__main__':
    make_rollup_tables()

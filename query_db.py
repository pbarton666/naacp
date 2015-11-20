
import psycopg2
import csv
import os
from pprint import pprint as pp
import numpy as np
#
from logger_setup import logger
from database import login_info
import load_test_tables

if True:  # constants, globals used for source code folding only
    
    #used to initialize np array
    NP_ROWS=1179  * 1179   #origin, dest pairs
    NP_COLS=20                     #aggregate columns required for each income group
    
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
        load_test_tables.create_test_tables (create_single_scenario=True)
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
    
   #transit metrics. These exclude fares (picked up from fares_fares table)    
    transit_time_metrics=['xferwaittime', 'walktime', 'railtime', 'initialwaittime', 'bustime', 'autotime']
    transit_other_metrics=['autodistance']
    transit_metrics=transit_time_metrics + transit_other_metrics
    
   
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

    We do this by coming up with a bunch of SELECT statements like this pair:

    '--adding bus_rail_fares for hbw. wexpbus - outbound leg
    SELECT  DISTINCT
        test_fares_fares.origin,
        test_fares_fares.dest,
        test_yesred_nohwy_mode_choice_od.hbw_inc1_wexpbus * test_fares_fares.wexpbus_pk
     FROM 
         test_yesred_nohwy_mode_choice_od , test_fares_fares 
     WHERE  
        test_yesred_nohwy_mode_choice_od.origin=test_fares_fares.origin AND 
        test_fares_fares.dest=test_yesred_nohwy_mode_choice_od.dest 
    ORDER BY 
        test_fares_fares.origin, test_fares_fares.dest
        
    --adding bus_rail_fares for hbw. wexpbus - return leg
    SELECT  DISTINCT
        test_fares_fares.origin,
        test_fares_fares.dest,
        test_yesred_nohwy_mode_choice_od.hbw_inc1_wexpbus * test_fares_fares.wexpbus_pk
     FROM 
         test_yesred_nohwy_mode_choice_od , test_fares_fares 
     WHERE  
        test_yesred_nohwy_mode_choice_od.dest=test_fares_fares.origin AND 
        test_yesred_nohwy_mode_choice_od.origin=test_fares_fares.dest 
    ORDER BY 
        test_fares_fares.origin, test_fares_fares.dest

        """          
    #general info for this metric
    topic = 'bus_rail_fares'    
    trips_table = '{}mode_choice_od'.format(scenario)
    metrics_table= '{}fares_fares'.format(prefix)
    logger.info('Beginning aggregation of {} data'.format(topic))
    
    #initialize null np array
    npa=np.zeros(((np_rows-1)**2, 3))  #orig, dest, value
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
                    #          --adding {topic} for {purpose}. {mode} - {trip_leg} leg\n' 
                    select= '--adding {} for {}. {} - {} leg\n'.format(topic, purpose, mode, trip_leg)
                    #          SELECT DISTINCT
                    select += 'SELECT  DISTINCT\n '
                    #                 {metrics_table}.origin
                    select += '\t{}.origin,\n'.format(metrics_table)
                    #                 {metrics_table}.dest            
                    select += '\t{}.dest,\n'.format(metrics_table)   
                    #               '{trips_table}.{purpose}_{income}_{mode} * {metrics_table}.{mode}_{pk_flag} '
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
                    logger.debug('executing:\n' + select)
                    #some queries can't succeed because there are not tables to support them e.g., autodistance for wexpbus mode
                    good_table=True
                    try:                        
                        curs.execute(select)
                    except:
                        msg = '--Nevermind.  NOT adding {} for {}. {} - {} leg - no table to support it\n'
                        logger.debug(msg.format(topic, purpose, mode, trip_leg))
                        good_table=False
                        break
                    
                    if good_table:
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
                    
                    
                    #print(select)
                    #curs.execute(select)
                    #res=np.array(curs.fetchall())     
                    
                    
                    ##add the results to the last column of the aggregation array
                    ##grab the OD columns for the first result
                    #if fresh_npa_array:    
                        #npa=res
                        #fresh_npa_array=False
                    ##add only the last column of subsequent ones
                    #else:
                        #npa[:,-1]+=res[:,-1]
                 
                    #logger.info('writing data for {} {} for {}: {} {}  {}'.format(pk_flag, topic, scenario, income, purpose, mode))
                    #logger.debug('executing {}\n'.format(select))
    
    return npa


def aggregate_transit_metrics(scenario=None,   
                                                    income=None,  
                                                    purposes=purposes,      
                                                    purposes_round_trip=purposes_round_trip,
                                                    bus_modes=bus_modes, 
                                                    rail_modes=rail_modes,
                                                    np_rows=NP_ROWS,
                                                    topic=None):
    
    """Aggregate time costs for mass transit.  Roll up topics over purpose and mode.  
        Cf aggregate_bus_rail_fares() for more verbose documentation."""
    
    """Keeps topics (initialwaittime, bustime, etc.) separate for now.  For final analysis it may makes sense to consolodate 
         waiting:   initialwaittime, transfertime
         bust time: wexpbus, dexpbus, wbus, dbus
         train time: wrail, wcrail, drail, dcrail
         
         ... but it's easier to combine later than have to separate."""
    
    """Typical SELECTS
    
    --adding xferwaittime for hbw. wbus - outbound leg
    SELECT  DISTINCT
        test_yesred_nohwy_transit_od_timecost.origin,
        test_yesred_nohwy_transit_od_timecost.dest,
        test_yesred_nohwy_mode_choice_od.hbw_inc1_wbus * test_yesred_nohwy_transit_od_timecost.pk_wbus_xferwaittime
     FROM 
         test_yesred_nohwy_mode_choice_od , test_yesred_nohwy_transit_od_timecost 
     WHERE  
        test_yesred_nohwy_mode_choice_od.origin=test_yesred_nohwy_transit_od_timecost.origin AND 
        test_yesred_nohwy_transit_od_timecost.dest=test_yesred_nohwy_mode_choice_od.dest 
    ORDER BY 
        test_yesred_nohwy_transit_od_timecost.origin, test_yesred_nohwy_transit_od_timecost.dest
        
    --adding xferwaittime for hbw. wbus - return leg
    SELECT  DISTINCT
        test_yesred_nohwy_transit_od_timecost.origin,
        test_yesred_nohwy_transit_od_timecost.dest,
        test_yesred_nohwy_mode_choice_od.hbw_inc1_wbus * test_yesred_nohwy_transit_od_timecost.pk_wbus_xferwaittime
     
    --adding xferwaittime for hbw. wbus - return leg
    SELECT  DISTINCT
        test_yesred_nohwy_transit_od_timecost.origin,
        test_yesred_nohwy_transit_od_timecost.dest,
        test_yesred_nohwy_mode_choice_od.hbw_inc1_wbus * test_yesred_nohwy_transit_od_timecost.pk_wbus_xferwaittime
     FROM 
         test_yesred_nohwy_mode_choice_od , test_yesred_nohwy_transit_od_timecost 
     WHERE  
        test_yesred_nohwy_mode_choice_od.dest=test_yesred_nohwy_transit_od_timecost.origin AND 
        test_yesred_nohwy_mode_choice_od.origin=test_yesred_nohwy_transit_od_timecost.dest 
    ORDER BY 
        test_yesred_nohwy_transit_od_timecost.origin, test_yesred_nohwy_transit_od_timecost.dest
                                                    
     """
     
    #general info for this metric
    trips_table = '{}mode_choice_od'.format(scenario)  
    metrics_table= '{}transit_od_timecost'.format(scenario)
    logger.info('Beginning aggregation of {} data'.format(topic))
    
    #initialize null np array
    npa=np.zeros(((np_rows-1)**2, 3))  #orig, dest, value
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
                    #          --adding {topic} for {purpose}. {mode} - {trip_leg} leg\n' 
                    select= '--adding {} for {}. {} - {} leg\n'.format(topic, purpose, mode, trip_leg)                         

                    #          SELECT DISTINCT
                    select += 'SELECT  DISTINCT\n '
                    #                 {metrics_table}.origin
                    select += '\t{}.origin,\n'.format(metrics_table)
                    #                 {metrics_table}.dest            
                    select += '\t{}.dest,\n'.format(metrics_table)   
                    
                    #               '{trips_table}.{purpose}_{income}_{mode} * {metrics_table}.{pk_flag}_{mode}_{topic} '
                    stmt=       '\t{}.{}_{}_{} * {}.{}_{}_{}\n '
                    
                    select+= stmt.format(trips_table, purpose, income, mode, metrics_table, pk_flag, mode, topic)
                    #print(select)
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
                    logger.debug('executing:\n' +select)
                    
                    #some queries can't succeed because there are not tables to support them e.g., autodistance for wexpbus mode
                    good_table=True
                    try:                        
                        curs.execute(select)
                    except:
                        msg = '--Nevermind.  NOT adding {} for {}. {} - {} leg - no table to support it\n'
                        logger.debug(msg.format(topic, purpose, mode, trip_leg))
                        good_table=False
                        break
                    
                    if good_table:
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
    return npa

def aggregate_hwy_costs(scenario=None,income=None)    :
    pass
    
    
def add_to_master(npa=None, master_npa=None, master_col=None, include_od_cols=False):
    "adds newly-acquired data column to the master array"
   
    if include_od_cols:
        #if we're going to add OD columns, that implies a new master, so override initialization
        master_npa=np.zeros((len(npa), NP_COLS))
        #load all three columns
        for col in range(0, 3):
            master_npa[:,col]=npa[:,col]

    else:
        #just load the values
        master_npa[:,master_col]+=npa[:,-1]
    return master_npa

def make_rollup_tables():

    for scenario in   scenarios:
        scenario=prefix + scenario+'_'
        
        for income in incomes:
            
            #initialize np array
            master_npa=np.zeros(((NP_ROWS-1)**2, NP_COLS)) 
            master_col =0 #numpy column index
            master_headers=['origin', 'dest']
            
            ##************** bus/rail fares **************#
            ##add orig, dest columns to master_npa, since this is the first one loaded
            #col_name = 'bus_rail_fares'                 #column name in master_npa array
            #routine=aggregate_bus_rail_fares      #the method run to process this
            
             ##add orig, dest columns to master_npa, since this is the first one loaded
            #master_npa = add_to_master( master_npa=master_npa,
                                                             #npa=routine(scenario=scenario, income=income), 
                                                             #master_col=master_col, 
                                                             #include_od_cols=True)
            #master_headers.append(col_name)
            #master_col +=3  #accounts for addition of od cols
            #logger.info('done rolling up {} {} {}'.format(scenario, income, col_name))
        
            
            #**************bus/ time time, distance**************#
            #create an aggregate across all puropses, times, 
            for metric in transit_metrics:   #walktime, etc.
                col_name='time'
                routine=aggregate_transit_metrics
                #npa=routine(scenario=scenario, income=income, topic = metric)
                master_npa = add_to_master( master_npa=master_npa,
                                                                 npa=routine(scenario=scenario, income=income, topic = metric), 
                                                                 master_col=master_col, 
                                                                 include_od_cols=False
                                                                 )  
                master_headers.append(col_name)
                master_col +=1
                logger.info('done rolling up {} {} {}'.format(scenario, income, col_name))                
            

            
            ##TODO: make db table for master


if __name__=='__main__':
    make_rollup_tables()

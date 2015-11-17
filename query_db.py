
import psycopg2
import csv
import os
from pprint import pprint as pp
import numpy as np
import logging
import logging.handlers


import utils_and_settings
from database import login_info

logger = logging.getLogger('trans_logger')

LOG_FILE='db_loader.log'
#set to 'WARN' to capure only data loading issues.  'DEBUG' is verbose.
LOG_LEVEL='INFO'   

#**** login credentials need to be updated in database.py ***


#**********************************************************


h = logging.handlers.RotatingFileHandler(LOG_FILE, 
                                         'a', 
                                         maxBytes=10*1024*1024, 
                                         backupCount=5)
formatter = logging.Formatter('%(asctime)s - %(levelname)s - '
                              '%(filename)s:%(lineno)s - %(message)s',
                              datefmt="%Y-%m-%d %H:%M:%S")
h.setFormatter(formatter)
logger.addHandler(h)
logger.setLevel(LOG_LEVEL)


#db=build_all.DB
db='test'
prefix='test_'  #used only for testing

#switches to limit queries for testing
LIMIT_ON = 10                #arbitrary, small >=1
LIMIT_OFF= 100000000         #arbitrary, large
LIMIT_STG =  ' LIMIT {}'
TEST=True                    #

conn = psycopg2.connect(database= db,
                        user=login_info['user'],
                        password=login_info['password']
                        )
curs = conn.cursor() 

""" 
 - Quality assurance on the underlying data
 - the amount of transportation consumed in each of these scenarios (trips, miles traveled, person miles traveled?)

-Aggregation of the time involved (weight time, bus time, train time, etc.)
 - aggregation of the cost involved under each scenario
 """
#For reference, create a few objects containg db objects.  The table names are like
#'nored_yeshwy_tod_od' and  'nored_yeshwy_person_trip_od' ... so we'll make these objects:
#
#  scenarios  { 'nored_yeshwy', etc.}
#  table_root {'person_trip_od', 'tod_od', etc.}
#  tables     {'nored-yeshwy_person_trip_od'} 
#
#... making coding a bit more foolproof.  Names are based on data dirs in build_all.py.

#table=None

def query_gen(table=None, query=None, cols=None, condition=None, test=TEST):
    "Generates queries.  Returns the sql"
    if not table: print('please specify a table')

    if test:
        limit = LIMIT_STG.format(LIMIT_ON)
    else:
        limit= LIMIT_STG.format(LIMIT_OFF)

    #Build the query.  If provided, go with that + any limit.
    if query:
        sql = query + ' FROM {}'.format(table) + limit
        return sql

    #... otherwise create a SELECT statement 
    sql= 'SELECT {} FROM {} '.format(cols, table)
    if condition:
        sql+= condition
    sql += limit
    return sql



queries=[]

table='nored_yeshwy_loaded_hwy_od_timecost'
"""Aggregate all time spent in transit by income group"""
scenarios=[('yesred_nohwy', 0), ('yesred_yeshwy', 0), ('nored_nohwy', 0), ('nored_nohwy', 0)]
incomes=[('inc1', 1), ('inc2', 2), ('inc3', 3), ('inc4', 4), ('inc5', 5)]
rail_modes=[('wrail', 1), ('wcrail', 2), ('drail', 3), ('dcrail', 4)]
bus_modes=[('wexpbus', 5), ('wbus', 6), ('dexpbus', 7), ('dbus', 8)]
drive_modes=[('da', 9), ('sr2', 10), ('sr3', 11)]
purposes_pk=[('hbw', 1), ('nhbw', 2)]
purposes_op=[('obo', 3), ('hbs', 4), ('hbo', 5)]
purposes = purposes_pk + purposes_op

times=[('xferwaittime', 1), ('walktime', 2), ('railtime', 3), ('initialwaittime', 4), ('bustime', 5), ('autotime', 6)]
placeholders=[('fare', 7), ('autodistance',8)]  #used as placeholders
occupancy_hwy_loaded=[('hov', 1), ('sov', 2)]
occupancy_hwy_tod=[('sov', 1), ('hov2', 2), ('hov3', 3)]
tod_hwy_loaded=[('am', 1), ('md', 2), ('pm', 3), ('nt', 4)]
tod_transit_times=[('pk', 1), ('op', 2)]
tod_fares=tod_transit_times
metrics=[('toll', 1), ('time', 2), ('distance', 3)]

def make_rollup_tables():

    for scenario,  _  in   [scenarios[0]]:
        scenario=prefix + scenario+'_'
        
        for income, _ in incomes:
            
            master_npa=None #placeholder for array to be transferred to the db
            master_headers=[]
            
            '''This is pretty straighforward:  rail/bus costs = SUM((peak trips * peak fare) + (op trips * op fare))
            
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
                     test_yesred_nohwy_.origin, test_yesred_nohwy_mode_choice_od.dest'''     
            

            #rail/bus costs = SUM((peak trips * peak fare) + (op trips * op fare))            
            npa=None        #create an empty npa array placeholder 
            while True:
                trips_table = '{}mode_choice_od'.format(scenario)
                fares_table= '{}fares_fares'.format(prefix)
                #peak
                pk_flag='pk'
                logger.info('{} transit costs'.format(pk_flag))
                for purpose, _ in purposes_pk:
                    for mode, _ in bus_modes+rail_modes:
                        #write a new SELECT statement for each purpose, mode pair
                        select= '--adding peak rail/bus fare costs for {}. {}\n'.format(purpose, mode)
                        select += 'SELECT  DISTINCT\n '
                        #origin and dest
                        select += '\t{}.origin,\n'.format(trips_table)
                        select += '\t{}.dest,\n'.format(trips_table)                    
                        stmt=       '\t{}.{}_{}_{} * {}.{}_pk\n '
                        select+= stmt.format(trips_table, purpose, income, mode, fares_table, mode)
                        #print(select)
                        select += 'FROM \n\t{} , {} \n '.format( trips_table, fares_table)
                        select +='WHERE  \n\t{}.origin={}.origin AND \n'.format( trips_table, fares_table)
                        select +='\t{}.dest={}.dest \n'.format(trips_table, fares_table)
                        select +='ORDER BY \n\t{}.origin, {}.dest\n\n'.format( trips_table,trips_table)                    
    
                        #grab the results. create a new np array or add this to the last (results) column                        
                        logger.info('writing data for {} transit fares for {}: {} {}  {}'.format(pk_flag, scenario, income, purpose, mode))
                        logger.debug('executing {}\n'.format(select))
                        print(select)
                        curs.execute(select)
                        res=np.array(curs.fetchall())
                        if npa is  None:
                            npa=res
                        else: 
                            npa[:,-1]+=res[:,-1]
                #off peak
                pk_flag='op'
                logger.info('{} transit costs'.format(pk_flag))
                for purpose, _ in purposes_op:
                    for mode, _ in bus_modes+rail_modes:
                        #write a new SELECT statement for each purpose, mode pair
                        select= '--adding {} rail/bus fare costs for {}, {}\n'.format(pk_flag, purpose, mode)
                        select += 'SELECT  DISTINCT\n '
                        #origin and dest
                        select += '\t{}.origin,\n'.format(trips_table)
                        select += '\t{}.dest,\n'.format(trips_table)                    
                        stmt=       '\t{}.{}_{}_{} * {}.{}_op\n '
                        select+= stmt.format(trips_table, purpose, income, mode, fares_table, mode)
                        #print(select)
                        select += 'FROM \n\t{} , {} \n '.format( trips_table, fares_table)
                        select +='WHERE  \n\t{}.origin={}.origin AND \n'.format( trips_table, fares_table)
                        select +='\t{}.dest={}.dest \n'.format(trips_table, fares_table)
                        select +='ORDER BY \n\t{}.origin, {}.dest'.format( trips_table,trips_table)                    
    
                        #grab the results. create a new np array or add this to the last (results) column                        
                        logger.info('writing data for {} transit fares for {}: {} {}  {}'.format(pk_flag, scenario, income, purpose, mode))
                        logger.debug('executing {}\n'.format(select))
                        #print(select)
                        curs.execute(select)
                        res=np.array(curs.fetchall())
                        if npa is  None:
                            npa=res
                        else: 
                            npa[:,-1]+=res[:,-1]
            
                #Add this to master npa for transfer to the db.
                if master_npa is  None:
                    master_npa=npa
                else: 
                    master_npa[:,-1]+=npa[:,-1]    
                master_headers.append('transit_costs_{}'.format(income))
                    
                logger.info('done rolling up transit costs for {} {}'.format(scenario, income))
                break  #done with transit costs                
          
            #highway distances (same logic for toll and drive time)
            '''            
            Distance = trips * distance / trip.  
            
            Trips come mode_choice_od.  Work-related trips are assumed on-peak.   
            
            Distances come from loaded_hwy_od_timecost.  Times are (am, pm, md, nt).  One question is how best to map the tod-based costs (toll, distance, drive time)
            to our cobbled-together notion of on/off peak.
            
            Thinking that this assumption isn't highly-leveraged for the analysis - unless the tod of trips is driven by the Red Line or highway upgrades.
            
            Mappings:  For on-peak trips, we'll assume costs are an average of the am and pm costs.  For off-peak trips, we'll assume an average of md and nt costs.
            
                              Modes 'sr2' and 'sr3' are 'hov' occupancies;  mode 'da' is 'sov' occupancy
                        
            
        
            '''
            #note that we're already in an income loop
            while True:
                
                #do three passes - one each for distance, toll, and hwy time
                
                trips_table = '{}mode_choice_od'.format(scenario)
                costs_table= '{}loaded_od_hwy_timecost'.format(scenario)
                               
                
                for metric in ['toll', 'time', 'distance']:
                    npa=None
                    #peak
                    pk_flag='pk' 
                    logger.info('{}'.format(metric))
                    for purpose, _ in purposes_pk:
                        for mode, _ in drive_modes:
                            for occupancy, _  in occupancy_hwy_loaded:
                                if mode=='da':
                                    loaded_occupancy='sov'
                                else:
                                    loaded_occupancy='hov'
                                    
                                #write a new SELECT statement for each purpose, mode pair
                                select= '\n--adding {} {} for {}, {}, {}\t\n'.format(pk_flag, metric, purpose, mode, occupancy)
                                select += 'SELECT  DISTINCT\n '
                                
                                #origin and dest
                                select += '\t{}.origin,\n'.format(trips_table)
                                select += '\t{}.dest,\n'.format(trips_table)    
                                
                                #here, we need to average the am and pm values
                                    #{trips_table}.{purpose}_{income}_{mode} * {cost_table}_hwy_{metric}_{loaded_occupancy}_{tod} /2 +  \n}
                                stmt1=       '\t{}.{}_{}_{} * {}.hwy_{}_{}_am  /2 +\n '
                                stmt2=       '\t{}.{}_{}_{} * {}.hwy_{}_{}_pm  /2  \n'
                                c=1
                                select+= stmt1.format(trips_table, purpose, income, mode,  costs_table, metric, loaded_occupancy)
                                select+= stmt2.format(trips_table, purpose, income, mode,  costs_table, metric, loaded_occupancy)
                                #print(select)
                                select += 'FROM \n\t{} , {} \n '.format( trips_table, costs_table)
                                select +='WHERE  \n\t{}.origin={}.origin AND \n'.format( trips_table, costs_table)
                                select +='\t{}.dest={}.dest \n'.format(trips_table, costs_table)
                                select +='ORDER BY \n\t{}.origin, {}.dest\n\n'.format( trips_table,trips_table)                    
        
                                #grab the results. create a new np array or add this to the last (results) column                        
                                logger.info('writing data for {} {} for {}: {} {}{} {}'.format(pk_flag, metric, scenario, income, purpose, mode, loaded_occupancy))
                                logger.debug('executing {}\n'.format(select))
                                #print(select)
                                curs.execute(select)
                                res=np.array(curs.fetchall())

                    #off peak
                    pk_flag='op' 
                    logger.info('{}'.format(metric))
                    for purpose, _ in purposes_pk:
                        for mode, _ in drive_modes:
                            for occupancy, _  in occupancy_hwy_loaded:
                                if mode=='da':
                                    loaded_occupancy='sov'
                                else:
                                    loaded_occupancy='hov'
                                    
                                #write a new SELECT statement for each purpose, mode pair
                                select= '\n--adding {} {} for {}, {}, {}\t\n'.format(pk_flag, metric, purpose, mode, occupancy)
                                select += 'SELECT  DISTINCT\n '
                                
                                #origin and dest
                                select += '\t{}.origin,\n'.format(trips_table)
                                select += '\t{}.dest,\n'.format(trips_table)    
                                
                                #here, we need to average the nt and md values
                                    #{trips_table}.{purpose}_{income}_{mode} * {cost_table}_hwy_{metric}_{loaded_occupancy}_{tod} /2 +  \n}
                                stmt1=       '\t{}.{}_{}_{} * {}.hwy_{}_{}_nt  /2 +\n '
                                stmt2=       '\t{}.{}_{}_{} * {}.hwy_{}_{}_md  /2  \n'
                                c=1
                                select+= stmt1.format(trips_table, purpose, income, mode,  costs_table, metric, loaded_occupancy)
                                select+= stmt2.format(trips_table, purpose, income, mode,  costs_table, metric, loaded_occupancy)
                                #print(select)
                                select += 'FROM \n\t{} , {} \n '.format( trips_table, costs_table)
                                select +='WHERE  \n\t{}.origin={}.origin AND \n'.format( trips_table, costs_table)
                                select +='\t{}.dest={}.dest \n'.format(trips_table, costs_table)
                                select +='ORDER BY \n\t{}.origin, {}.dest\n\n'.format( trips_table,trips_table)                    
        
                                #grab the results. create a new np array or add this to the last (results) column                        
                                logger.info('writing data for {} {} for {}: {} {}{} {}'.format(pk_flag, metric, scenario, income, purpose, mode, loaded_occupancy))
                                logger.debug('executing {}\n'.format(select))
                                #print(select)
                                curs.execute(select)
                                res=np.array(curs.fetchall())
                                #this   adds the combined peak + off-peak totals
                                if npa is  None:
                                    npa=res
                                else: 
                                    npa[:,-1]+=res[:,-1]
                                True
                    break
                    #Add this to master npa for transfer to the db.
                    if master_npa is  None:
                        master_npa=npa
                    else: 
                        master_npa[:,-1]+=npa[:,-1]   
                    master_headers.append('{} {} {}'.format(scenario, income, metric))
                        
                logger.info('done rolling up highway costs for {} {}'.format(scenario, income))
                break  #done with highway costs    



make_rollup_tables()

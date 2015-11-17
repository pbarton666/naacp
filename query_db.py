
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
LOG_LEVEL='DEBUG'   

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
logger.setLevel(logging.DEBUG)

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


        #rail/bus costs = SUM((peak trips * peak fare) + (op trips * op fare))
        #Produces a bunch of SELECTS like this:
        '''--adding peak rail/bus fare costs
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
        
        
        #create an empty np array for now
        npa=None
        
        #Gather data for each income group.  Handle off peak and peak separately (different fares
        #  and work-related purposes imply peak travel)
        for income, _ in incomes:
            
            master_npa=None
            
            #transit fare cost (tod, across purpose, mode pair) - 
            while True:
                trips_table = '{}mode_choice_od'.format(scenario)
                fares_table= '{}fares_fares'.format(prefix)
                #peak
                logger.info('peak transit costs')
                for purpose, _ in purposes_pk:
                    for mode, _ in bus_modes+rail_modes:
                        #write a new SELECT statement for each purpose, mode pair
                        select= '--adding peak rail/bus fare costs\n'
                        select += 'SELECT  DISTINCT\n '
                        #origin and dest
                        select += '\t{}.origin,\n'.format(trips_table)
                        select += '\t{}.dest,\n'.format(trips_table)                    
                        stmt=       '\t{}.{}_{}_{} * {}.{}_pk\n '
                        select+= stmt.format(trips_table, purpose, income, mode, fares_table, mode)
                        print(select)
                        select += 'FROM \n\t{} , {} \n '.format( trips_table, fares_table)
                        select +='WHERE  \n\t{}.origin={}.origin AND \n'.format( trips_table, fares_table)
                        select +='\t{}.dest={}.dest \n'.format(trips_table, fares_table)
                        select +='ORDER BY \n\t{}.origin, {}.dest\n\n'.format( trips_table,trips_table)                    
    
                        #grab the results. create a new np array or add this to the last (results) column                        
                        logger.info('writing data for peak transit fares for {}: {} {}{}'.format(scenario, income, purpose, mode))
                        logger.debug('executing {}\n'.format(select))
                        print(select)
                        curs.execute(select)
                        res=np.array(curs.fetchall())
                        if npa is  None:
                            npa=res
                        else: 
                            npa[:,-1]+=res[:,-1]
                #peak
                logger.info('off-peak transit costs')
                for purpose, _ in purposes_op:
                    for mode, _ in bus_modes+rail_modes:
                        #write a new SELECT statement for each purpose, mode pair
                        select= '--adding peak rail/bus fare costs\n'
                        select += 'SELECT  DISTINCT\n '
                        #origin and dest
                        select += '\t{}.origin,\n'.format(trips_table)
                        select += '\t{}.dest,\n'.format(trips_table)                    
                        stmt=       '\t{}.{}_{}_{} * {}.{}_op\n '
                        select+= stmt.format(trips_table, purpose, income, mode, fares_table, mode)
                        print(select)
                        select += 'FROM \n\t{} , {} \n '.format( trips_table, fares_table)
                        select +='WHERE  \n\t{}.origin={}.origin AND \n'.format( trips_table, fares_table)
                        select +='\t{}.dest={}.dest \n'.format(trips_table, fares_table)
                        select +='ORDER BY \n\t{}.origin, {}.dest'.format( trips_table,trips_table)                    
    
                        #grab the results. create a new np array or add this to the last (results) column                        
                        logger.info('writing data for peak transit fares for {}: {} {}{}'.format(scenario, income, purpose, mode))
                        logger.debug('executing {}\n'.format(select))
                        print(select)
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
                    
                logger.info('done rolling up transit costs for {} {}'.format(scenario, income))
                break  #done with transit costs                
          
            #transit fare cost (peak, across purpose, mode pair)
            while True:
                trips_table = '{}mode_choice_od'.format(scenario)
                fares_table= '{}fares_fares'.format(prefix)
                for purpose, _ in purposes_pk:
                    for mode, _ in bus_modes+rail_modes:
                        #write a new SELECT statement for each purpose, mode pair
                        select= '\n\n--adding peak rail/bus fare costs\n'
                        select += 'SELECT  DISTINCT\n '
                        #origin and dest
                        select += '\t{}.origin.\n'.format(trips_table)
                        select += '\t{}.dest,\n'.format(trips_table)                    
                        stmt=       '\t{}.{}_{}_{} * {}.{}_pk\n '
                        select+= stmt.format(trips_table, purpose, income, mode, trips_table, mode)
                        select += 'FROM \n\t{}{} , {}.{}.{}  \n '.format(scenario, trips_table, fares_table, mode, '_pk')
                        select +='WHERE  \n\t{}.origin={}.origin AND \n'.format( trips_table, fares_table)
                        select +='\t{}.dest={}.dest \n'.format(trips_table, fares_table)
                        select +='ORDER BY \n\t{}.origin, {}.dest\n\n'.format(scenario, trips_table,trips_table)                    
    
                        #grab the results. create a new np array or add this to the last (results) column                        
                        logger.info('writing data for peak transit fares for {}: {} {}{}'.format(scenario, income, purpose, mode))
                        logger.debug('executing {}\n'.format(select))
                        curs.execute(select)
                        res=np.array(curs.fetchall())
                        if npa is  None:
                            npa=res
                        else: 
                            npa[:,-1]+=res[:,-1]
                #off peak, across purpose, mode pair           
                for purpose, _ in purposes_op:
                    for mode, _ in bus_modes+rail_modes:
                        #write a new SELECT statement for each purpose, mode pair
                        select= '\n\n--adding peak rail/bus fare costs\n'
                        select += 'SELECT  DISTINCT\n '
                        #origin and dest
                        select += '\t{}.origin.\n'.format(trips_table)
                        select += '\t{}.dest,\n'.format(trips_table)                    
                        stmt=       '\t{}.{}_{}_{} * {}.{}_op\n '   #changed to reflect op
                        select+= stmt.format(trips_table, purpose, income, mode, trips_table, mode)
                        select += 'FROM \n\t{}{} , {}.{}.{}  \n '.format(scenario, trips_table, fares_table, mode, '_pk')
                        select +='WHERE  \n\t{}.origin={}.origin AND \n'.format( trips_table, fares_table)
                        select +='\t{}.dest={}.dest \n'.format(trips_table, fares_table)
                        select +='ORDER BY \n\t{}.origin, {}.dest\n\n'.format(scenario, trips_table,trips_table)                    
    
                        #grab the results. create a new np array or add this to the last (results) column
                        logger.info('writing data for off peak transit fares for {}: {} {}{}').format(scenario, income, purpose, mode)
                        logger.debug('executing {}\n'.format(select))                        
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
                    
                logger.info('done rolling up transit costs for {} {}').format(scenario, income)
                break  #done with transit costs    



make_rollup_tables()

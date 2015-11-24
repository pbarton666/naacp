
import psycopg2
import csv
import os
import sys
import traceback
from pprint import pprint as pp
import numpy as np
import logging

from database import login_info
import load_test_tables
import logger_setup
from logger_setup import logger

from value_constants import *


def  add_dlta_cons_surplus(a):
        "returns 0.5 * (trips_base + trips_trial) * (cost_base-cost_trial)   i.e.,   .5 * (q1 + q2) * (p1-p2)"
        cost_base=a[:,2]
        trips_base=a[:,3]
        cost_trial=a[:,4]  
        trips_trial=a[:,5]

                #base, trips base, benefit trial, trips trial    
        dlta_cs = 0.5 * (trips_base + trips_trial )*(cost_base - cost_trial)
        a[:,6] = dlta_cs
        return a    

if True:  # constants, globals used for source code folding only

        #used to initialize np array
        NP_ROWS=1179  * 1179   #origin, dest pairs
        NP_COLS=100                    #aggregate columns required for each income group

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
        base_scenario = 'nored_nohwy'
        test_scenarios=['nored_yeshwy', 'yesred_nohwy']
        scenarios=[base_scenario] + test_scenarios
        incomes=['inc1', 'inc2', 'inc3', 'inc4', 'inc5']
        rail_modes=['wrail', 'wcrail', 'drail', 'dcrail']
        bus_modes=['wexpbus', 'wbus', 'dexpbus', 'dbus']
        drive_modes=['da', 'sr2', 'sr3']
        purposes_pk=['hbw', 'nhbw']
        purposes_op=['obo', 'hbs', 'hbo']
        purposes = purposes_pk + purposes_op
        #double-check this with Carl - could be that 'hbo' is also businesss, but probably school, etc.
        purposes_biz=['obo']                                                            #'on the clock' trips
        purposes_personal = ['hbw', 'nhbw', 'hbs', 'hbo']        #everything else


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

        tod_hwy_loaded = ['am', 'pm', 'md', 'nt']
        tod_transit_times=['pk', 'op']
        tod_fares=tod_transit_times
        metrics=['toll', 'time', 'distance']    

        #mappings
        purpose_peak_flag={}  #mapped to abbreviations in tables
        for p in purposes_pk:
                purpose_peak_flag[p] = 'pk'
        for p in purposes_op:
                purpose_peak_flag[p] = 'op'



def rollup_bus_rail_fares(scenario=None,   
                          base_scenario=None,
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

    --finding bus_rail_fares for hbw. wexpbus - outbound leg
SELECT  DISTINCT
 	test_fares_fares.origin,
	test_fares_fares.dest,
	test_nored_nohwy_mode_choice_od.hbw_inc1_wexpbus * test_fares_fares.wexpbus_pk,
 	test_nored_nohwy_mode_choice_od.hbw_inc1_wexpbus
 FROM 
	 test_nored_nohwy_mode_choice_od , test_fares_fares 
 WHERE  
	test_nored_nohwy_mode_choice_od.origin=test_fares_fares.origin AND 
	test_fares_fares.dest=test_nored_nohwy_mode_choice_od.dest 
ORDER BY 
	test_fares_fares.origin, test_fares_fares.dest

        """          
        #general info for this metric


        topic = 'bus_rail_fares'    
        base_trips_table = '{}_mode_choice_od'.format(  base_scenario)
        test_trips_table = '{}_mode_choice_od'.format(  scenario)
        base_metrics_table= '{}fares_fares'.format(prefix)
        test_metrics_table= '{}fares_fares'.format(prefix)    
        cols_added =5  #metric base, trips base, metric trial, trips trial, benefit


        logger.info('Beginning aggregation of {} fares data'.format(topic))
        logger.info("Trips using {}\n and {}".format(base_trips_table, test_trips_table))
        logger.info("Costs using {}\n and {}".format(base_metrics_table, test_metrics_table))	

        export_array=np.zeros(((np_rows-1)**2, 3))
        this_export_array_col = -1

        #loop over purposes and bus/rail modes, accumulating the atomistic consumer surplus changes, including
        #    outbound + return for home-based trips

        for purpose in purposes:
                #peak or off peak as f(purpose) 
                pk_flag=purpose_peak_flag[purpose]

                #round trip or one-way? (round trip for home based journeys)
                trip_legs=['outbound']
                if purpose in purposes_round_trip:
                        trip_legs.append('return')

                #loop over all trips for both bus and rail modes
                for mode in bus_modes+rail_modes:        

                        logger.info('beginning benefit calcs for {} {} mode'.format(purpose, mode))

                        #calculate benefits for each leg of the trip separately; combine the benefits from a round-trip at the end 
                        #     of the 'trip_leg' loop.

                        need_npa_combined = True  #holds outbound+return benefits rollup

                        #flag for npa creation
                        this_np_col =-1               

                        #construct the similar SELECT for both base and test scenarios (different tables), store results in np array 'npa',
                        #    calculate separately the benefits for each leg then roll the benefits up in np array 'combined_npa'

                        #for each leg of the trip do a base versus trial case benefits calculation
                        for trip_leg in trip_legs:          

                                if this_np_col<0:
                                        npa=np.zeros(((np_rows-1)**2, cols_added+2))  #scratch array                

                                #this selects trips and costs from the base and trial case tables
                                for metrics_table, trips_table, name in zip([base_metrics_table, test_metrics_table],
                                                                            [base_trips_table, test_trips_table],
                                                                            ['base', scenario]):

                                        logger.info('running {} case for {} {}  {}'.format(name,  purpose, mode, trip_leg))


                                        #          --finding {topic} for {purpose}. {mode} - {trip_leg} leg\n' 
                                        select= '--finding {} for {}. {} - {} leg - scenario: {}\n'.format(topic, purpose, mode, trip_leg, name)
                                        select += 'SELECT  DISTINCT\n '
                                        #                 {metrics_table}.origin
                                        select += '\t{}.origin,\n'.format(metrics_table)
                                        #                 {metrics_table}.dest            
                                        select += '\t{}.dest,\n'.format(metrics_table)   
                                        #               '{trips_table}.{purpose}_{income}_{mode} * {metrics_table}.{mode}_{pk_flag} '
                                        stmt=       '\t{}.{}_{}_{} * {}.{}_{},\n '
                                        select+= stmt.format(trips_table, purpose, income, mode, metrics_table, mode, pk_flag)
                                        #               '{trips_table}.{purpose}_{income}_{mode} '
                                        stmt=       '\t{}.{}_{}_{}\n '                        
                                        select+= stmt.format(trips_table, purpose, income, mode)

                                        #                FROM {trips_table} , {metrics_table}
                                        select += 'FROM \n\t {} , {} \n '.format( trips_table, metrics_table)

                                        if trip_leg== 'outbound':      #use OD pairs from trip table same as metric table's
                                                #               WHERE  {trips_table}.origin={metrics_table}.origin AND 
                                                select +='WHERE  \n\t{}.origin={}.origin AND \n'.format( trips_table, metrics_table)
                                                #                   {metrics_table}.dest={metrics_table}.dest)
                                                select +='\t{}.dest={}.dest \n'.format(metrics_table, trips_table)

                                        else:            #use transposed OD pairs from trip table (origin = metrics.dest, dest=metrics.origin)       
                                                #               WHERE  {trips_table}.dest={metrics_table}.origin AND 
                                                select +='WHERE  \n\t{}.dest={}.origin AND \n'.format( trips_table, metrics_table)
                                                #                   {metrics_table}.origin={metrics_table}.dest)
                                                select +='\t{}.origin={}.dest \n'.format(trips_table, metrics_table)                        

                                        #             ORDER BY {metrics_table}.origin, {metrics_table}.dest
                                        select +='ORDER BY \n\t{}.origin, {}.dest\n\n'.format( metrics_table, metrics_table)   
                                        logger.debug(select)
                                        curs.execute(select)

                                        res=np.array(curs.fetchall())     

                                        #This rolls up the costs and trips from both scenarios:  npa is rows of:  origin, dest, benefit base, trips base, benefit trial, trips trial 
                                        if   this_np_col < 0:
                                                #add first 4 columns of result to a the first 4 columns of the scratch array  (origin, dest, base cost, base trips)
                                                npa[:,:4]=res
                                                this_np_col=4
                                        else:
                                                #add the cost, trips columns from the result to cols 4-6 of the scratch array (trial cost, trial trips)
                                                npa[:,4:6] = res[:,-2:]
                                                this_np_col+=2
                                                #calculate the benefits
                                                logger.info('calculating delta cs for {} {} {} '.format(scenario, purpose, mode))
                                                npa= add_dlta_cons_surplus(npa)


                                        #npa_combined rolls up the atomized benefits, calculated separately for each leg of the journey. 
                                        if need_npa_combined:
                                                npa_combined = npa
                                                need_npa_combined=False
                                                logger.info('adding benefits to new npa_combined array')
                                        else:
                                                #otherwise add the benefits from the second leg (the last column) to the combined_npa array
                                                npa_combined[:,-1]+=npa[:,-1]                       
                                                logger.info('done with both legs; adding return leg to npa_combined:     {}  {} '.format(purpose, mode))

                        #add consumer surplus improvements to the export array

                        if this_export_array_col < 0:
                                #not yet created; add the orig and destin columns, along with the cs deltas
                                export_array[:,:2]=npa_combined[:,:2]
                                export_array[:,-1]=npa_combined[:,-1]
                                this_export_array_col=3
                                logger.debug('creating new export array')
                        else:
                                #... otherwise just add the new benfits to the cumulative total
                                export_array[:,-1]+=npa_combined[:,-1]
                                logger.info('adding additional cs deltas to export array')
                logger.info('Done with mode {}\n\n'.format(mode))
        logger.info('Done with purpose {}'.format(purpose))

        return export_array                


def rollup_transit_metrics(scenario=None,   
                           base_scenario=None,
                           income=None,  
                           purposes=purposes,      
                           master_col=None,
                           master_header=None,
                           purposes_round_trip=purposes_round_trip,
                           bus_modes=bus_modes, 
                           rail_modes=rail_modes,
                           np_rows=NP_ROWS,
                           topic=None):

        """Aggregate time costs for mass transit.  Roll up topics over purpose and mode.  
        Cf aggregate_bus_rail_fares() for more verbose documentation."""

        """Keeps topics (initialwaittime, bustime, etc.) separate for now.  For final analysis it may makes sense to consolodate 
         waiting:   initialwaittime, transfertime
         bus time: wexpbus, dexpbus, wbus, dbus
         train time: wrail, wcrail, drail, dcrail

         ... but it's easier to combine later than have to separate."""

        """Typical SELECTS

     """

        #general info for this metric

        scenario =scenario + '_'
        base_scenario = base_scenario + '_'

        base_trips_table = '{}mode_choice_od'.format(base_scenario)  
        test_trips_table = '{}mode_choice_od'.format(scenario)  
        base_metrics_table= '{}transit_od_timecost'.format(base_scenario)
        test_metrics_table= '{}transit_od_timecost'.format(scenario)
        cols_added =5  #metric base, trips base, metric trial, trips trial, benefit

        logger.info('\n\n\n***** Beginning aggregation of transit data for {}'.format(topic))
        logger.info("Trips using {}\n and {}".format(base_trips_table, test_trips_table))
        logger.info("Costs using {}\n and {}".format(base_metrics_table, test_metrics_table))

        #this array will aggreate the info gleaned here
        export_array=np.zeros(((np_rows-1)**2, 3))
        this_export_array_col = -1	

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

                #loop thru appropriate modes and compose SELECT
                for mode in bus_modes+rail_modes:      

                        logger.info('beginning benefit calcs for {} {} {}'.format(purpose, mode, topic))

                        #calculate benefits for each leg of the trip separately; combine the benefits from a round-trip at the end 
                        #     of the 'trip_leg' loop.

                        need_npa_combined = True  #holds outbound+return benefits rollup

                        #flag for npa creation
                        this_np_col =-1                  


                        #calculate each leg of the trip separately  
                        for trip_leg in trip_legs:

                                if this_np_col<0:
                                        npa=np.zeros(((np_rows-1)**2, cols_added+2))  #scratch array       

                                #this selects from the base and trial case tables
                                for metrics_table, trips_table, name in zip([base_metrics_table, test_metrics_table],
                                                                            [base_trips_table, test_trips_table],
                                                                            ['base', scenario]):

                                        logger.info('running {} case for {} {}  {} using {} and {}'.format(name,  purpose, mode, trip_leg, trips_table, metrics_table)   )     

                                        #create SELECT statements

                                        #          --adding {topic} for {purpose}. {mode} - {trip_leg} leg\n' 
                                        select= '--adding {} for {}. {} - {} leg\n'.format(topic, purpose, mode, trip_leg)                         

                                        select += 'SELECT  DISTINCT\n '
                                        #                 {metrics_table}.origin                                                        origin
                                        select += '\t{}.origin,\n'.format(metrics_table)
                                        #                 {metrics_table}.dest                                                           destination
                                        select += '\t{}.dest,\n'.format(metrics_table)   

                                        #               '{trips_table}.{purpose}_{income}_{mode} * {metrics_table}.{pk_flag}_{mode}_{topic} '                      metric
                                        stmt=       '\t{}.{}_{}_{} * {}.{}_{}_{},\n '
                                        select+= stmt.format(trips_table, purpose, income, mode, metrics_table, pk_flag, mode, topic)

                                        #               '{trips_table}.{purpose}_{income}_{mode} '                                                                                                            trips
                                        stmt=       '\t{}.{}_{}_{}\n '                        
                                        select+= stmt.format(trips_table, purpose, income, mode)					


                                        #print(select)
                                        #                FROM {trips_table} , {metrics_table}
                                        select += 'FROM \n\t {} , {} \n '.format( trips_table, metrics_table)

                                        if trip_leg== 'outbound':     #use OD pairs from trip table same as metric table's

                                                #               WHERE  {trips_table}.origin={metrics_table}.origin AND 
                                                select +='WHERE  \n\t{}.origin={}.origin AND \n'.format( trips_table, metrics_table)
                                                #                   {metrics_table}.dest={metrics_table}.dest)
                                                select +='\t{}.dest={}.dest \n'.format(metrics_table, trips_table)

                                        else:            #use transposed OD pairs from trip table (origin = metrics.dest, dest=metrics.origin)

                                                #               WHERE  {trips_table}.dest={metrics_table}.origin AND 
                                                select +='WHERE  \n\t{}.dest={}.origin AND \n'.format( trips_table, metrics_table)
                                                #                   {metrics_table}.origin={metrics_table}.dest)
                                                select +='\t{}.origin={}.dest \n'.format(trips_table, metrics_table)                        

                                        #             ORDER BY {metrics_table}.origin, {metrics_table}.dest
                                        select +='ORDER BY \n\t{}.origin, {}.dest\n\n'.format( metrics_table, metrics_table)   

                                        logger.debug(select)
                                        try:
                                                good_table=True
                                                curs.execute(select)
                                        except:
                                                #some empty columns  were not produced  (e.g., wexpbus_autodistance) because they don't apply						
                                                relations=[  '\t{}.{}_{}_{} * {}.{}_{}_{},\n '.format(trips_table, purpose, income, mode, metrics_table, pk_flag, mode, topic),
                                                             '\t{}.{}_{}_{}\n '  .format(trips_table, purpose, income, mode)
                                                             ]												
                                                logger.warning('This SELECT failed, probably because the data is n/a:  {}'.format('  '.join(relations)))
                                                good_table=False
                                                #close out any
                                                curs.execute('END')


                                        #if the query failed, we've busted out; so go to the next mode	
                                        if good_table:

                                                res=np.array(curs.fetchall())     

                                                #This rolls up the costs and trips from both scenarios:  npa is rows of:  origin, dest, benefit base, trips base, benefit trial, trips trial 
                                                if   this_np_col < 0:
                                                        #add first 4 columns of result to a the first 4 columns of the scratch array  (origin, dest, base cost, base trips)
                                                        npa[:,:4]=res
                                                        this_np_col=4
                                                else:
                                                        #add the cost, trips columns from the result to cols 4-6 of the scratch array (trial cost, trial trips)
                                                        npa[:,4:6] = res[:,-2:]
                                                        this_np_col+=2
                                                        #calculate the benefits
                                                        logger.info('calculating delta cs for {} {} {} '.format(scenario, purpose, mode))
                                                        npa= add_dlta_cons_surplus(npa)


                                                #npa_combined rolls up the atomized benefits, calculated separately for each leg of the journey. 
                                                if need_npa_combined:
                                                        npa_combined = npa
                                                        need_npa_combined=False
                                                        logger.info('adding benefits to new npa_combined array')
                                                else:
                                                        #otherwise add the benefits from the second leg (the last column) to the combined_npa array
                                                        npa_combined[:,-1]+=npa[:,-1]                       
                                                        logger.info('done with both legs; adding return leg to npa_combined:     {}  {} '.format(purpose, mode))
                        #   next mode at col 12     

                        #if a mode fails to produce a clean query, don't bother trying to add the info
                        if good_table:
                                if this_export_array_col < 0:
                                        #not yet created; add the orig and destin columns, along with the cs deltas
                                        export_array[:,:2]=npa_combined[:,:2]
                                        export_array[:,-1]=npa_combined[:,-1]
                                        this_export_array_col=3
                                        logger.debug('creating new export array')
                                else:
                                        #... otherwise just add the new benfits to the cumulative total
                                        export_array[:,-1]+=npa_combined[:,-1]
                                        logger.info('adding additional cs deltas to export array')

                ###		
                logger.info('Done with mode {}\n\n'.format(mode))
        logger.info('Done with purpose {}'.format(purpose))      

        return export_array


def rollup_hwy_metrics(scenario=None,   
                       income=None,  
                       purposes=purposes,   
                       master_header=None,
                       purposes_round_trip=purposes_round_trip,
                       np_rows=NP_ROWS,
                       topic=None,
                       occupancy=None):

        """Aggregate costs for highway-only travel.  Roll up topics over purpose, tod.  
        Keeps occupancies (sov, hov2, hov3) and costs (time, distance, toll) separate"""

        """Typical SELECTS


     """

        """   
                    #     hwy_toll_hov_am
                #     hbo_inc1_md_hov3"""     
        #general info for this metric
        base_trips_table = '{}trips_purpose_income_tod_occ'.format(base_scenario)  
        test_trips_table = '{}trips_purpose_income_tod_occ'.format(scenario)  
        base_metrics_table= '{}loaded_hwy_od_timecost'.format(base_scenario)
        test_metrics_table= '{}loaded_hwy_od_timecost'.format(scenario)

        #We'll need to leverage the time to reflect vehicle occupancy.  Since the topic (metric) is an
        #  input parameter, we can calculate a uniform multiplier here.  Occupancy leverages only time.
        if topic=='time':
                mult = time_adjust[occupancy]
        else:
                mult=1

        cols_added =5  #metric base, trips base, metric trial, trips trial, benefit

        logger.info('\n\n\n***** Beginning aggregation of highway data for {}'.format(topic))
        logger.info("Trips using {}\n and {}".format(base_trips_table, test_trips_table))
        logger.info("Costs using {}\n and {}".format(base_metrics_table, test_metrics_table))

        #this array will aggreate the info gleaned here
        export_array=np.zeros(((np_rows-1)**2, 3))
        this_export_array_col = -1	

        #initialize null np array
        npa=np.zeros(((np_rows-1)**2, 3))  #orig, dest, value
        fresh_npa_array=True	


        logger.info('Beginning aggregation of {} data'.format(topic))

        #we're passing in topic (metric), occupancy and income, purpose (grouped by business/personal as input, analyzed
        #   atomisticly here and provided as aggregate as output values)

        for purpose in purposes:  #rolls up whatever list of purposes provided (allows biz/personal segregation)

                #round trip of one-way (round trip for home based journeys)?
                trip_legs=['outbound']
                if purpose in purposes_round_trip:
                        trip_legs.append('return')

                for tod in tod_hwy_loaded:  #'am', 'pm', 'md', 'nt'

                        logger.info('beginning benefit calcs for {} {} {} {}'.format(purpose, tod, occupancy, topic))

                        need_npa_combined = True  #holds outbound+return benefits rollup

                        #flag for npa creation
                        this_np_col =-1  				

                        #calculate benefits for each leg of the trip separately; combine the benefits from a round-trip at the end 
                                                #     of the 'trip_leg' loop.
                        for trip_leg in trip_legs:      

                                if this_np_col<0:
                                        npa=np.zeros(((np_rows-1)**2, cols_added+2))  #scratch array       

                                #this selects from the base and trial case tables
                                for metrics_table, trips_table, name in zip([base_metrics_table, test_metrics_table],
                                                                            [base_trips_table, test_trips_table],
                                                                            ['base', scenario]):	

                                        #          --adding {topic} for {purpose} {tod} {occupancy}- {trip_leg} leg\n' 
                                        select= '--adding {} for {} {} {} - {} leg\n'.format(topic, purpose, tod, occupancy, trip_leg)                         
                                        select += 'SELECT  DISTINCT\n '
                                        #                 {metrics_table}.origin
                                        select += '\t{}.origin,\n'.format(metrics_table)
                                        #                 {metrics_table}.dest            
                                        select += '\t{}.dest,\n'.format(metrics_table)   

                                        #               '{trips_table}.{purpose}_{income}_{tod}_{occ} * {metrics_table}.hwy_{topic}_{occupancy}_{tod} * {mult}'
                                        stmt=       '\t{}.{}_{}_{}_{} * {}.hwy_{}_{}_{} * mult \n '    #mult leverages time for HOVs

                                        select+= stmt.format(trips_table, purpose, income, tod, occupancy, \
                                                             metrics_table, topic, occupancy, tod)

                                                #               '{trips_table}.{purpose}_{income}_{tod}_{occ} '
                                        stmt=       '\t{}.{}_{}_{}_{}'

                                        select+= stmt.format(trips_table, purpose, income, tod, occupancy)										


                                        #                FROM {trips_table} , {metrics_table}
                                        select += 'FROM \n\t {} , {} \n '.format( trips_table, metrics_table)

                                        if trip_leg== 'outbound':                   #use OD pairs from trip table same as metric table's

                                                #               WHERE  {trips_table}.origin={metrics_table}.origin AND 
                                                select +='WHERE  \n\t{}.origin={}.origin AND \n'.format( trips_table, metrics_table)
                                                #                   {metrics_table}.dest={metrics_table}.dest)
                                                select +='\t{}.dest={}.dest \n'.format(metrics_table, trips_table)

                                        else:               #use transposed OD pairs from trip table (origin = metrics.dest, dest=metrics.origin)

                                                #               WHERE  {trips_table}.dest={metrics_table}.origin AND 
                                                select +='WHERE  \n\t{}.dest={}.origin AND \n'.format( trips_table, metrics_table)
                                                #                   {metrics_table}.origin={metrics_table}.dest)
                                                select +='\t{}.origin={}.dest \n'.format(trips_table, metrics_table)                        

                                        #             ORDER BY {metrics_table}.origin, {metrics_table}.dest
                                        select +='ORDER BY \n\t{}.origin, {}.dest\n\n'.format( metrics_table, metrics_table)   

                                        logger.debug(select)
                                        try:
                                                good_table=True
                                                curs.execute(select)
                                        except:
                                                #some empty columns  were not produced  (e.g., wexpbus_autodistance) because they don't apply						
                                                relations=[  
                                                        '\t{}.{}_{}_{}_{} * {}.hwy_{}_{}_{} * mult \n '.format(trips_table, purpose, income, tod, occupancy, metrics_table, topic, occupancy, tod),
                                                        '\t{}.{}_{}_{}'	  .format(trips_table, purpose, income, tod)
                                                ]
                                                logger.warning('This SELECT failed, probably because the data is n/a:  {}'.format('  '.join(relations)))
                                                good_table=False
                                                #close out any
                                                curs.execute('END')

                                        #if the query failed, we've busted out; so go to the next tod	
                                        if good_table:

                                                res=np.array(curs.fetchall())     

                                                #This rolls up the costs and trips from both scenarios:  npa is rows of:  origin, dest, benefit base, trips base, benefit trial, trips trial 
                                                if   this_np_col < 0:
                                                        #add first 4 columns of result to a the first 4 columns of the scratch array  (origin, dest, base cost, base trips)
                                                        npa[:,:4]=res
                                                        this_np_col=4
                                                else:
                                                        #add the cost, trips columns from the result to cols 4-6 of the scratch array (trial cost, trial trips)
                                                        npa[:,4:6] = res[:,-2:]
                                                        this_np_col+=2
                                                        #calculate the benefits
                                                        logger.info('calculating delta cs for {} {} {} '.format(scenario, purpose, mode))
                                                        npa= add_dlta_cons_surplus(npa)

                                                #npa_combined rolls up the atomized benefits, calculated separately for each leg of the journey. 
                                                if need_npa_combined:
                                                        npa_combined = npa
                                                        need_npa_combined=False
                                                        logger.info('adding benefits to new npa_combined array')
                                                else:
                                                        #otherwise add the benefits from the second leg (the last column) to the combined_npa array
                                                        npa_combined[:,-1]+=npa[:,-1]                       
                                                        logger.info('done with both legs; adding return leg to npa_combined:     {}  {} '.format(purpose, mode))
                                                #   next mode at col 12     

                #if a mode fails to produce a clean query, don't bother trying to add the info
                if good_table:
                        if this_export_array_col < 0:
                                #not yet created; add the orig and destin columns, along with the cs deltas
                                export_array[:,:2]=npa_combined[:,:2]
                                export_array[:,-1]=npa_combined[:,-1]
                                this_export_array_col=3
                                logger.debug('creating new export array')
                        else:
                                #... otherwise just add the new benfits to the cumulative total
                                export_array[:,-1]+=npa_combined[:,-1]
                                logger.info('adding additional cs deltas to export array')

                        ###		
                        logger.info('Done with mode {}\n\n'.format(mode))
                logger.info('Done with purpose {}'.format(purpose))   
                return export_array

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

def do_rollup_bus_rail_fares(base = None, scenario=None, income=None, master_npa=None, master_col=None, master_headers=None, include_od_columns=False):
        "rolls up the atomistic imporvements from fare changes"
        routine=rollup_bus_rail_fares

        col_name = 'bus_rail_fares_{}'.format(income)                 #column name in master_npa array
        routine=rollup_bus_rail_fares      #the method run to process this
        
        master_npa = add_to_master( master_npa=master_npa,
                                        npa=routine(scenario=scenario, base_scenario=base, income=income), 
                                        master_col=master_col, 
                                        include_od_cols=include_od_columns)
        master_headers.append(col_name)
        if include_od_columns:
                master_col +=3  #accounts for addition of od cols
        else:
                master_col += 1
        logger.info('done rolling up {} {} {}'.format(scenario, income, col_name))
        
        return(master_headers, master_npa, master_col)

def do_rollup_transit_metrics(base=None, scenario=None, income=None, master_npa=None,  master_col=None, master_headers=None, purposes=None, metric=None, name=None, include_od_columns=False):
        "rolls up the atomistic improvements in transit metrics, adds them to master_npa array"
        
        routine=rollup_transit_metrics
        col_name=name + '_'+ metric   + '_'+ income     
        npa=routine(scenario=scenario, base_scenario=base,  income=income, topic = metric, purposes=purposes)
        master_npa = add_to_master( master_npa=master_npa,
                                    npa=npa,
                                    master_col=master_col, 
                                    include_od_cols=False
                                    )  
        master_headers.append(col_name )
        if include_od_columns:
                master_col +=3  #accounts for addition of od cols
        else:
                master_col += 1
        logger.info('done rolling up transit metrics {} {} {}'.format(scenario, income, name))                 
        
        return(master_headers, master_npa, master_col)

def do_rollup_hwy_metrics(base=None, scenario=None, occupancy=None, income=None, master_npa=None, master_header=None, master_col=None, purposes=None, metric=None, include_od_columns=False):
        "rolls up the atomistic improvements in hwy metrics, adds them to master_npa array"
        
        routine=rollup_hwy_metrics        
        col_name=metric+ '_' +occupancy

        npa=routine(scenario=scenario, income=income, topic = metric, occupancy=occupancy)

        master_npa = add_to_master( master_npa=master_npa,
                                    npa=npa, 
                                    master_col=master_col, 
                                    include_od_cols=False
                                    )  
        master_header.append(col_name)
        if include_od_columns:
                master_col +=3  #accounts for addition of od cols
        else:
                master_col += 1

                logger.info('done rolling up hwy metrics {} {} {}'.format(scenario, income, metric))       

        return(master_header, master_npa, master_col)

def make_rollup_tables( ):
        """This rolls up atomisitc improvements in cs between the base scenario and alternatives.  The
           result is a new db table for each scerario.  Columns are aggregates of the cs improvments for
           individual categories e.g., bus/rail fares."""

        base=prefix + base_scenario        #prefix is just a flag used for testing - null string in production
        for scenario in test_scenarios:    
                scenario=prefix + scenario
                master_headers=['origin', 'dest']
                #name of db table to save
                tname =scenario + '_benefits'                   

                for income in incomes:                     
 
                        #initialize np array; will hold all results for this scenario
                        master_npa=np.zeros(((NP_ROWS-1)**2, NP_COLS)) 
                        master_col =0 #numpy column index
                        
                        #roll up rail + bus fares by income group.
                       # o_rollup_bus_rail_fares(base = None, scenario=None, income=None, master_npa=None,include_od_columns=False)
                        master_header,  master_npa, master_col = do_rollup_bus_rail_fares(base = base, scenario=scenario, income=income, master_npa=master_npa,   master_col=master_col, include_od_columns=True, master_headers=master_headers)
                        
                        #transit metrics (distance, toll, time) are calculated separately by type of travel (personal, biz) and metric
                        for purposes, name  in zip ([purposes_biz, purposes_personal],
                                                                        ['business', 'personal']
                                                                        ):
                                for metric in transit_metrics:   #walktime, etc.    
                                        master_headers,  master_npa, master_col  = do_rollup_transit_metrics(base=base, scenario=scenario, income=income, master_npa=master_npa, master_col=master_col, master_headers=master_headers, purposes=purposes, metric=metric, name=name, include_od_columns=False)

                        #highway (auto-only) metrics are calculated separately by type of travel (personal, biz) , occupancy and metri	
                        for purposes, name  in zip (
                                [purposes_biz, purposes_personal],
                                 ['business', 'personal']):
                                
                                for metric in metrics:   #toll, distance, time
                                        for occupancy in occupancy_hwy_tod:  #sov, hov2, hov3
                                                master_header,  master_npa, master_col  =do_rollup_hwy_metrics(base=base, scenario=scenario, occupancy=occupancy,  income=income, master_npa=master_npa, master_header=master_header, master_col=master_col, purposes=purposes, metric=metric, include_od_columns=False)

                #income done
                logger.info('done with income group {}'.format(income))
                
        #scenario done
        logger.info('done with scenario {}'.format(scenario)	)

        db_table_name='aggregated_{}_{}'   .format(scenario, income)
        create_db_table(tname=db_table_name, scenario=scenario, income=income, 
                        data=master_npa, master_headers=master_headers)
        logger.debug('Success in creating aggregate db table for {} {}'.format(scenario, income))
        a=1

def xmake_rollup_tables():
        """This rolls up atomisitc improvements in cs between the base scenario and alternatives.  The
           result is a new db table for each scerario.  Columns are aggregates of the cs improvments for
           individual categories e.g., bus/rail fares."""

        #note:  race demograhics anticipated to be in table race_race.  Stored by origin only    

        base=prefix + base_scenario        #prefix is just a flag used for testing - null string in production
        for scenario in test_scenarios:    
                scenario=prefix + scenario
                master_headers=['origin', 'dest']

                for income in incomes:
                        tname =scenario + '_' + income + '_benefits'    
                        #initialize np array
                        master_npa=np.zeros(((NP_ROWS-1)**2, NP_COLS)) 
                        master_col =0 #numpy column index

                        if True:  #include for source code folding only

                                #************** bus/rail fares **************#
                                #add orig, dest columns to master_npa, if  this is the first one loaded
                                ##TODO: consider changing the following for production
                                include_od_columns=True
                                col_name = 'bus_rail_fares_{}'.format(income)                 #column name in master_npa array
                                routine=rollup_bus_rail_fares      #the method run to process this

                                        #add orig, dest columns to master_npa, since this is the first one loaded
                                master_npa = add_to_master( master_npa=master_npa,
                                                                npa=routine(scenario=scenario, base_scenario=base, income=income), 
                                                                master_col=master_col, 
                                                                include_od_cols=include_od_columns)
                                master_headers.append(col_name)
                                if include_od_columns:
                                        master_col +=3  #accounts for addition of od cols
                                else:
                                        master_col += 1
                                logger.info('done rolling up {} {} {}'.format(scenario, income, col_name))

                        if True:  #include for source code folding only				

                                #**************bus/ rail time, distance**************#
                                #create an aggregate across all puropses, times, 
                                ##TODO:  False for production
                                include_od_columns=False 	

                                routine=rollup_transit_metrics
                                #adding purposes loop so we can track work-related ('obo') and personal for differential VOT
                                for purposes, name  in zip ([purposes_biz, purposes_personal],
                                                                                ['business', 'personal']
                                                                                ):
                                        for metric in transit_metrics:   #walktime, etc.
                                                col_name=name + '_'+ metric   + '_'+ income     
                                                npa=routine(scenario=scenario, base_scenario=base,  income=income, topic = metric, purposes=purposes)
                                                master_npa = add_to_master( master_npa=master_npa,
                                                                                                        npa=npa,
                                                                                                        master_col=master_col, 
                                                                                                        include_od_cols=False
                                                                                                        )  
                                                master_headers.append(col_name )
                                                if include_od_columns:
                                                        master_col +=3  #accounts for addition of od cols
                                                else:
                                                        master_col += 1
                                                logger.info('done rolling up transit metrics {} {} {}'.format(scenario, income, name))         

                        if True:

                                #**************highway distance/time**************#
                                #create an aggregate across all puropses, times, 

                                ##TODO:  False for production
                                include_od_columns=False 		

                                routine=rollup_hwy_metrics
                                #adding purposes loop so we can track work-related ('obo') and personal for differential VOT

                                for purposes, name  in zip ([purposes_biz, purposes_personal],
                                                            ['business', 'personal']
                                                            ):												

                                        for metric in metrics:   #toll, distance, time

                                                for occupancy in occupancy_hwy_tod:  #sov, hov2, hov3

                                                        col_name=metric+ '_' +occupancy

                                                        npa=routine(scenario=scenario, income=income, topic = metric, occupancy=occupancy)

                                                        master_npa = add_to_master( master_npa=master_npa,
                                                                                    npa=npa, 
                                                                                    master_col=master_col, 
                                                                                    include_od_cols=False
                                                                                    )  
                                                        master_headers.append(col_name)
                                                        if include_od_columns:
                                                                master_col +=3  #accounts for addition of od cols
                                                        else:
                                                                master_col += 1

                                                        logger.info('done rolling up hwy metrics {} {} {}'.format(scenario, income, name))       
                #income done
                logger.info('done with income group {}'.format(income))
        #scenario done
        logger.info('done with scenario {}'.format(scenario)	)

        db_table_name='aggregated_{}_{}'   .format(scenario, income)
        create_db_table(tname=db_table_name, scenario=scenario, income=income, 
                        data=master_npa, master_headers=master_headers)
        logger.debug('Success in creating aggregate db table for {} {}'.format(scenario, income))
        a=1

def create_db_table(tname=None, scenario=None, income=None, master_headers=None, data=None):
        "creates a new table for this income group and scenario"

        curs.execute('END')
        sql = 'DROP TABLE IF EXISTS {}'.format(tname)
        curs.execute(sql)
        logger.debug('dropping table {}'.format(tname)) 
        sql='CREATE TABLE IF NOT EXISTS {} ('.format(tname)
        for col in master_headers:  
                sql+=' {} float,'.format(str(col))        
        sql = sql[:-1] + ');' #
        print(sql)
        curs.execute(sql)
        conn.commit()      

        ##TODO:  should be a clever way simply to load the array (alt: dump file and load; also could just do INSERTs)

        #for line in data:
                #sql="INSERT INTO {} (".format(tname) 
                #for col in master_headers:  
                        #sql+=' {}, '.format(col)   
                #sql=sql[:-2] + ") VALUES ("
                #for value in line.split(','):
                        #sql+=' {},'.format(value)            
                #sql=sql[:-2] + ") "
                #print(sql)
                #curs.execute(sql)      

        pass

if __name__=='__main__':
        make_rollup_tables()


import unittest
import psycopg2
import os
from pprint import pprint as pp
import numpy as np

import utils_and_settings
from database import login_info

#module to be tested
import query_db
#test table building routines
import load_test_tables

db='test' 
prefix='test_'  #makes unequivocally-named tables.  Set to empty string after dev

conn = psycopg2.connect(database= db,
                        user=login_info['user'],
                        password=login_info['password']
                        )
curs = conn.cursor() 

class TestQueries (unittest.TestCase):

   def test_fare_rollup(self):
      """"test roll-up of fares logic - particularly the transposition of OD pairs to simulate return
       leg of home-based trips"""
      
      #test for some scenario and income group
      scenario='test_yeshwy_yesred_'
      income='inc1'
      
      #these provide substitutes for more complete ones in the module
      purposes=['hbw']
      purposes_round_trip=['hbw']
      bus_modes=[]
      rail_modes=['dcrail'] 
      
      #use these tables
      trips_table = '{}mode_choice_od'.format(scenario)
      trip_col='hbw_inc1_dcrail'
      
      metrics_table= '{}fares_fares'.format(prefix)   
      metric_col='dcrail_pk'
         
      #create trips table with known values (values to table values based on OD pair:  10*O+D)

      load_test_tables.load_specific_table(tname=trips_table, this_col=trip_col)
   
      #create metric table. 
      vals=[]
      #add some values
      for origin in range(1, load_test_tables.TEST_ROWS+1):
         for dest in range(1, load_test_tables.TEST_COLS+1):
            vals.append(900+10*origin+dest)
            #vals.append(1)
      a=1
      load_test_tables.load_specific_table(tname=metrics_table, this_col=metric_col, vals=vals)
      
      """
      this gives us two tables:
      trips:                                     fares:
      origin  dest  hbw_dcrail        origin dest   dcrail_pk
         1         1           11                1       1        911
         1         2           21                1       2        921
         ...
         3         2           32                3       2        931
         3         3           33                3       3        933
       WHERE  
	test_yeshwy_yesred_mode_choice_od.dest=test_fares_fares.origin AND 
	test_fares_fares.origin=test_yeshwy_yesred_mode_choice_od.dest 
      """
      #Call the aggregation routine.  It returns a NP array containing OD values for
      #    outbound_trips * fares + return_trips * fares
   
      npa=query_db.aggregate_bus_rail_fares(scenario=scenario, 
                                                                       income=income, 
                                                                       purposes=purposes,
                                                                       purposes_round_trip=purposes_round_trip,
                                                                       bus_modes=bus_modes,
                                                                       rail_modes=rail_modes,
                                                                       np_rows=9)
         
      '''
      The resulting array should be:
      origin      dest      outbound   +  return (OD switched)
      1               1         11 * 911    +  11 * 911   
      1               2         12 * 912    +  21 * 912   
      ...
      3               2         32 * 932    +  23 * 932    
      3               3         33 * 933    +  33 * 933  
      
      More specifically:
      
      Outbound						                        Return							
      trips			        fare	        cost		        trips	                fare	               cost			
      orig	dest	trips	cost/trip	trips*cost		orig	dest	trips	cost/trip	       trips*cost		total fare both legs	
      1	      1	11	911      	10021		        1	    1	 11	911	              10021		    20042	
      1	      2	12	912      	10944	        	1  	2	 21	912	              19152		    30096	
      1	      3	13	913	        11869	         	1  	3	 31	913	              28303		    40172	
      2	      2	22	922	      	20284	         	2	    2	 22	922	              20284		    40568	
      3	      1	31	931	      	28861	         	3	    1	 13	931	              12103		    40964	
      3	      2	32	932	      	29824	         	3  	2	 23	932	              21436		    51260	
      3	      3	33	933        	30789	         	3  	3	 33	933	              30789		    61578	

      
      '''
      target=np.array((
         [[  1.00000000e+00,   1.00000000e+00 ,  2.00420000e+04],
          [  1.00000000e+00,   2.00000000e+00 ,  3.00960000e+04],
          [  1.00000000e+00 ,  3.00000000e+00 ,  4.01720000e+04],
          [  2.00000000e+00 ,  1.00000000e+00 ,  3.03930000e+04],
          [  2.00000000e+00 ,  2.00000000e+00 ,  4.05680000e+04],
          [  2.00000000e+00 ,  3.00000000e+00 ,  5.07650000e+04],
          [  3.00000000e+00,   1.00000000e+00 ,  4.09640000e+04],
          [  3.00000000e+00 ,  2.00000000e+00 ,  5.12600000e+04],
          [  3.00000000e+00 ,  3.00000000e+00  , 6.15780000e+04]]
         ))
      self.assertTrue(npa.all(), target.all())
      a=1



if __name__=='__main__':
    
    unittest.main()
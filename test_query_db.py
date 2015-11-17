"""Tests that all the data tables have been loaded.  """
import unittest
import psycopg2
import os
from pprint import pprint as pp

import utils_and_settings
import build_all
from database import login_info
import query_db

db='test' 
prefix='test_'  #makes unequivocally-named tables.  Set to empty string after dev

conn = psycopg2.connect(database= db,
                        user=login_info['user'],
                        password=login_info['password']
                        )
curs = conn.cursor() 

dtype='float'

#Categories of information found in the tables and a value.  The values are inserted
#  into the tables, so we can know what to expect.
scenarios=[('yesred_nohwy', 0), ('yesred_yeshwy', 0), ('nored_nohwy', 0), ('nored_nohwy', 0)]

incomes=[('inc1', 1), ('inc2', 2), ('inc3', 3), ('inc4', 4), ('inc5', 5)]

rail_modes=[('wrail', 1), ('wcrail', 2), ('drail', 3), ('dcrail', 4)]
bus_modes=[('wexpbus', 5), ('wbus', 6), ('dexpbus', 7), ('dbus', 8)]
drive_modes=[('da', 9), ('sr2', 10), ('sr3', 11)]
all_modes=rail_modes+bus_modes+drive_modes

purposes_pk=[('hbw', 1), ('nhbw', 2)]
purposes_op=[('obo', 3), ('hbs', 4), ('hbo', 5)]
all_purposes = purposes_pk + purposes_op

pub_trans_times=[('xferwaittime', 1), ('walktime', 2), ('railtime', 3), ('initialwaittime', 4), ('bustime', 5), ('autotime', 6)]
placeholders=[('fare', 7), ('autodistance',8)]  #used as placeholders

occupancy_hwy_loaded=[('hov', 1), ('sov', 2)]
occupancy_hwy_tod=[('sov', 1), ('hov2', 2), ('hov3', 3)]

tod_hwy_loaded=[('am', 1), ('md', 2), ('pm', 3), ('nt', 4)]
tod_transit_times=[('pk', 1), ('op', 2)]
tod_fares=tod_transit_times

metrics=[('toll', 1), ('time', 2), ('distance', 3)]

def load_table(tname = None, cols=None, vals=None):
    

    
    curs.execute('END')
    curs.execute("drop table if exists {}".format(tname))    
    sql='CREATE TABLE IF NOT EXISTS {} ('.format(tname)
    
    cols = ['origin', 'dest'] + cols
    vals=['1', '-666'] + vals
    for col in cols:  
        sql+=' {} {},'.format(str(col), dtype)        
    sql = sql[:-1] + ');' #

    curs.execute(sql)
    conn.commit()  
    
    for dest in range(1, 5):
        vals[1]=str(dest) 

        sql="INSERT INTO {}  ( ".format(tname, cols) 
        for col in cols:  
            sql+=' {}, '.format(col)   
        sql=sql[:-2] + ") VALUES ("
        for val in vals:
            sql+=' {},'.format(val)            
        sql=sql[:-2] + ") "
        #pp(sql)
        curs.execute(sql)     
    conn.commit()
    

def create_test_tables():
    for scenario, _ in [scenarios[0]]:  #change to do all scenarios
        
        #'_hwy_od  tod, occ, metrics'
        while True:   #makes for easier source code folding
            tname= prefix + scenario + '_hwy_od'
            cols=[]
            vals=[]
            for tod, todval in tod_transit_times:
                for occupancy, oval in occupancy_hwy_loaded:
                    for metric, mval in metrics:
                        cols.append(tod + '_' + occupancy + '_' +  metric)
                        vals.append(todval+oval*10+mval*100)
            load_table(tname=tname, cols=cols, vals=vals)
            break        
 
        #'loaded_od_hwy_timecost   metric, occupancy, tod
        while True:  
            tname= prefix + scenario + '_loaded_od_hwy_timecost'
            cols=[]
            vals=[]
            for metric, mval in metrics:
                    for occupancy, oval in occupancy_hwy_loaded:
                        for tod, todval in tod_hwy_loaded:                            
                            cols.append('hwy_' + metric + '_' + occupancy + '_' +  tod)
                            vals.append(mval+oval*10+todval*100)
            load_table(tname=tname, cols=cols, vals=vals)
            break    
        
        #'tod_od:  purpose, TOD, occupancy, income
        while True:  
            tname= prefix + scenario + 'tod_od'
            cols=[]
            vals=[]
            
            for purpose, pval in all_purposes:
                for income, ival in incomes:
                    for tod, todval in tod_hwy_loaded:
                        for occupancy, oval in occupancy_hwy_tod:
                            cols.append( purpose + '_' + income + '_' + tod + '_' +  occupancy)
                            vals.append(pval+ival * 10 + todval+100+oval*1000)                        
            load_table(tname=tname, cols=cols, vals=vals)
            break          
        
        #'person_trip_od:  purpose,  income
        while True:  
            tname= prefix + scenario + 'person_trip_od'
            cols=[]
            vals=[]
            for purpose, pval in all_purposes:
                for income, ival in incomes:
                            cols.append( purpose + '_' + income )
                            vals.append(pval+ival * 10 )                        
            load_table(tname=tname, cols=cols, vals=vals)
            break     
        
        
        #'transit_od_timecost:   tod, mode, time
        while True:  
            tname= prefix + scenario + 'transit_od_timecost'
            cols=[]
            vals=[]
            for tod, todval in tod_transit_times:
                for mode, mval in rail_modes + bus_modes:
                    for time, tval in pub_trans_times:
                            cols.append(tod + '_' + mode + '_' + time )
                            vals.append(todval + mval*10 + tval * 100 )                        
            load_table(tname=tname, cols=cols, vals=vals)
            break           
        
        #fares:  mode, tod
        while True:  
            tname= prefix + 'fares_fares'
            cols=[]
            vals=[]
            for mode, mval in rail_modes + bus_modes:
                    for tod, todval in tod_transit_times:
                            cols.append(mode + '_' + tod  )
                            vals.append(mval + todval*10 + tval * 100 )                        
            load_table(tname=tname, cols=cols, vals=vals)
            break            
        
        
        #'mode_choice_od   purpose, income, mode '
        while True:   
            tname= prefix + scenario + '_mode_choice_od'
            cols=[]
            vals=[]
            for purpose, pval in all_purposes:
                for income, ival in incomes:
                    for mode, mval in all_modes:
                        cols.append(purpose + '_' + income + '_' +  mode)
                        vals.append(pval+ival+10+mval*100)
            load_table(tname=tname, cols=cols, vals=vals)
            break
           
create_test_tables()
print('done')


def get_limit(test):
    "returns LIMIT clause for query"
    #cheating a bit, but I want tests to be effective even w/ changed limit
    # values or format string
    if test:
        return query_db.LIMIT_STG.format(query_db.LIMIT_ON)
    else:
        return query_db.LIMIT_STG.format(query_db.LIMIT_OFF)


class Test(unittest.TestCase):
    def tester(self):
        "tests query builder"
        table = 'fares_fares'
        cols = ['wexpbus_pk', 'wrail_pk', 'wrail_op']
        condition = "WHERE origin=1 and dest = 1"
        query="SELECT COUNT(*)"
        test=True    

        #no query, all cols, no cond, no test
        sql =query_db.query_gen(table=table, query=None, cols=', '.join(cols), condition=None, test=test)
        target='SELECT wexpbus_pk, wrail_pk, wrail_op FROM fares_fares ' + get_limit(test)
        print(sql)
        self.assertEqual(sql, target)
        curs.execute(sql)

        #no query, one col, no cond, no test
        sql =query_db.query_gen(table=table, query=None, cols=cols[0], condition=None, test=test)
        target='SELECT wexpbus_pk FROM fares_fares ' + get_limit(test)
        print(sql)
        self.assertEqual(sql, target)  
        curs.execute(sql)

        #no query, all cols *, no cond, no test
        sql =query_db.query_gen(table=table, query=None, cols='*', condition=None, test=test)
        target='SELECT * FROM fares_fares ' + get_limit(test)
        print(sql)
        self.assertEqual(sql, target) 
        curs.execute(sql)

        #no query, one col, cond, no test
        sql =query_db.query_gen(table=table, query=None, cols=cols[0], condition=condition, test=test)
        target='SELECT wexpbus_pk FROM fares_fares WHERE origin=1 and dest = 1' + get_limit(test)
        print(sql)
        self.assertEqual(sql, target)  
        curs.execute(sql)

        #query, one col, cond, no test
        sql =query_db.query_gen(table=table, query=query, cols=cols[0], condition=condition, test=test)
        target='SELECT COUNT(*) FROM fares_fares' + get_limit(test)
        print(sql)
        self.assertEqual(sql, target)   
        curs.execute(sql)

        #query, one col, cond, test
        sql =query_db.query_gen(table=table, query=query, cols=cols[0], condition=condition, test=True)
        target='SELECT COUNT(*) FROM fares_fares' + get_limit(test)
        print(sql)
        self.assertEqual(sql, target)   
        curs.execute(sql)


if __name__=='__main__':
    create_test_tables ()
    unittest.main()
from  database import login_info
import psycopg2

DB='naacp'

 

def describe_db(db=DB):
    conn = psycopg2.connect(database = db,
                            user=login_info['user'],
                            password=login_info['password']
                            )
    curs = conn.cursor()    
    
    curs.execute("SELECT table_name FROM information_schema.tables WHERE table_schema = 'public';")
    #curs.execute('SHOW TABLES {}'.format(db))
    t_names = curs.fetchall()
    all_names=[]
    for item in t_names:
        all_names.append(item[0])
    clean_names=[]
    #this just moves origin and dest to front of the list
    for ix, name in enumerate(all_names[:]):
        if name=='origin':
            clean_names.append(name.pop(ix))
        if name=='dest':
            clean_names.append(name.pop(ix))
        
        
    for name in all_names:
        clean_names.append(name)
        
    print('*****************************************')
    print('These tables now are in the database {}'.format(db))
    
    #print out the table info and drop a copy in a file for future ref
    with open('database_info.txt', 'w') as f:
        for name in clean_names: 
            msg ='\nTable {}'.format(name)
            print(msg)
            f.write(msg + '\n')
            curs.execute("SELECT column_name FROM information_schema.columns WHERE table_name ='{}'".format(name))
            info = curs.fetchall()
            for tidbit in info:
                msg = '     {}'.format(tidbit[0])
                print(msg)
        
if __name__=='__main__':
    describe_db()
    ('dir_1_data',)
    ('dir_2_data',)
    ('test',)    
    
import numpy as np  

#origin, dest, cost base, trips base, cost trial, trips trial    
a = np.zeros((3,7))
a[0]=[1,10,1.0, 100, .50, 120,0]
a[1]=[10,1,2.0, 150, 1.50, 100,0]

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

if __name__=='__main__':

        print(a)
        print(add_dlta_cons_surplus(a))

"""
value constants.py

Provides value of time, etc. to be used to calculate consumer surplus changes.
"""

"""
Discussion and sources:

There is general guidance regarding value of time available from USDOT in this document:

https://www.transportation.gov/sites/dot.gov/files/docs/USDOT%20VOT%20Guidance%202014.pdf

Value of time savings (VTTS) depends on purpose, income, and the nature of how the time is spent.  It's
a function of opportunity cost (productivity or recreation).  Some of the guidance provided:

   - 'On the clock' biz travel valued as a function of annual (gross) compensation.
   
   - All travelers in HOVs are assumed to have independent, additive values. 
   
   - Purpose of trip doesn't matter.  DOT "retain(s) the assumption of fixed VTTS relationships for different trip 
      purposes and an income elasticity of 1.0 for all".  (p 10)

   - "For local personal travel, VTTS is estimated at 50 percent of hourly median household income" (p. 13)

   - "Personal time spent walking or waiting outside of vehicles, as well as time spent standing
      in vehicles or bicycling, should be evaluated at 100 percent of hourly income...." (p.16)
      
There is data on the cost of auto ownership from USDOT here:

http://www.rita.dot.gov/bts/sites/rita.dot.gov.bts/files/publications/national_transportation_statistics/html/table_03_17.html

     - the variable cost of driving an auto (2013, current-year) is 19 cents/mile.
     
The BLS has a CPI inflation calculator here:

http://data.bls.gov/cgi-bin/cpicalc.pl
     
******************     
Value of time:

The value of time reported by Tim ('vot' below) is pretty close to the gross salary at the midpoint of the ranges
provided.  For instance, the 'inc1' group makes $0-20K so has a midpoint of $10K.  Multiplying the VoT ($/hr) by working
hours in a year gives $10483.   So, I'm assuming that this is the something like the gross compensation recommended by DOT.

Accordingly, I'm using this as the time value for value of "other" ('obo') trips.

For the rest of the trips, this is discounted per DOT recommendations ('vot_cpm', below).

Income groups:

The MSTMUsersGuide (p. 14) discusses the houselhold income quintiles per the 2000 Census data, and in 1999 dollars.
I'm hoping that the the current model is run with 2010 Census data and 2015 dollars, but I'm a bit suspicious.

This could be material if the bottom quintiles are "wider" i.e., if there are more poor people in reality than modeled.

Driving costs:

Tim reports the cost per mile as 9.9 cents.  According to the Users Manual, that would be in year 2000 dollars.  
Extrapolating that to 2013 dollars (using the BLS CPI calculator) makes that 13.4 cents per mile in 2013 dollars.  This is a 
lot different than the DOT recommendations of 19 cents per mile in 2013 dollars, for the record.

For now, we'll Tim's value, inflated to 2015 dollars (a factor of 1.38) - 13.7 cents mile.  'drive_cpm' below

Fuel and Emissions:

Standards are available at:  http://www3.epa.gov/otaq/climate/documents/420f12051.pdf

Also, I'm not sure about these mpg values.  EPA reports that the current fleet average is 23.4 mpg, while the model uses 22 mpg (p. 75).  
That's pretty close, for today's averages but in 2030 that won't likely be the case.   

EPA is projecting that combined new cars+tricks in 2016 will get 35.5 mpg.  By 2025, this will be 54.5 mpg.  About double.
Without a lot of research, I can't say what the fleet mix, average age, fuel mix, etc. will be in 2030, but it won't look much like what's
in the model.  

For the sake of argument, let's say that the average car around in 2030 was made next year.  The delta benefit from 
reduced emissions and fuel cost would be overstated by about 50%, cetris paribus ( about .043 gal/mile versus .028).

Carbon content of gasoline from EIA available at:  http://www.eia.gov/tools/faqs/faq.cfm?id=307&t=11

A gallon of gasoline produces 19.64 pounds CO2, diesel 22.38 pounds, E(thanol)10 18.95, E100  12.73, B(iofuel)20 22.06, B100 20.77.
Using output for E10 below.

"""

DOT_ADJ= .5

#income groupings from MSTM (p. 16), vot from Tim (correspondence w/ Carl 4 Nov 2015). Cents per minute.
vot_cpm={'inc1':  8.4, 'inc2': 25, 'inc3':  41.7, 'inc4': 50.4, 'inc5': 106.4} 

#all  non-home based, 'other' trips, along with walking and waiting times are valued at "full retail", others are deflated  per DOT recommendations

vot_adjust=   {'walktime':                { 'obo':1.0, 'hbo': DOT_ADJ, 'hbw': 1.0, 'nhbw': 1.0, 'hbs':1.0} ,
                            'xferwaittime':         { 'obo':1.0, 'hbo': DOT_ADJ, 'hbw': 1.0, 'nhbw': 1.0, 'hbs':1.0} , 
                            'initialwaittime':     { 'obo':1.0, 'hbo': DOT_ADJ, 'hbw': 1.0, 'nhbw': 1.0, 'hbs':1.0} , 
                            'railtime':                  { 'obo':1.0, 'hbo': DOT_ADJ, 'hbw':  DOT_ADJ, 'nhbw': DOT_ADJ, 'hbs':DOT_ADJ} , 
                            'bustime':                  { 'obo':1.0, 'hbo':DOT_ADJ, 'hbw':  DOT_ADJ, 'nhbw': DOT_ADJ, 'hbs':DOT_ADJ} ,
                           }

#Inflates time spent by vehicle occupancy.  HOVs burn all occupants' time.                         
time_adjust= {'sov': 1, 'hov2': 2, 'hov3': 3}      

#drive cost per mile from Tim (correspondence w/ Carl 4 Nov 2015, 2000 dollars inflated to 2015 dollars)
drive_cpm=13.7

#fuel efficiency (per MSTMUsersGuide, p. 75)
gal_per_mile = 1/22.8

#carbon output, pounds per gallon, (assuming E10; using EIA value )
co2_per_gal=18.95

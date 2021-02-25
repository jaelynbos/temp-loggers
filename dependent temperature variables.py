# -*- coding: utf-8 -*-
"""
Created on Sat Feb 13 19:36:04 2021

@author: jaelyn
"""

"""
Dependent temperature variables
"""
import pandas as pd
import numpy as np

#Daily flux
def dmin(x,y):
    days = y['time'].dt.date
    daysu = y['time'].dt.date.unique()
    mins = y[days==x].min(axis=0)
    dm = pd.DataFrame([mins(daysu[i]) for i in np.arange(0,len(daysu),1)])
    return dm    

def dmax(x,y):
    days = y.dt.date
    daysu = y['time'].dt.date.unique()
    mins = y[days==x].max(axis=0)
    dm = pd.DataFrame([mins(daysu[i]) for i in np.arange(0,len(daysu),1)])
    return dm   
    
def dailydiff(noaadf):
    dd = [dmax(i,noaadf) - dmin(i,noaadf) for i in np.arange(0,len(noaadf),1)]
    return dd

dailydiffs = pd.DataFrame([dailydiff(noaa[i]) for i in np.arange(0,len(noaa),1)])

#Weekly standard deviation

def weeksd(x,y):
    weeks = y['time'].dt.week
    weeksu = y['time'].dt.week.unique()
    wsd = lambda x,y: y[weeks==x].sdt(axis=0)
    wsds = pd.DataFrame([wsd(weeksu[i]) for i in np.arange(0,len(weeksu),1)])
    return(wsds)

def weeklysd(noaadf):
    wsd = pd.DataFrame([weeksd(i,noaadf) for i in np.arange(0,len(noaadf),)])
    return wsd
 
weeklysds = pd.DataFrame([weeklysd(noaa[i]) for i in np.arange(0,len(noaa),1)])

#Annual 99%
def year99(x,y):
    year = y['time'].dt.year
    yearsu = y['time'].dt.year.unique()
    y99 = lambda x,y: y[year==x].quantile(q=0.99,axis=0)
    y99s = pd.DataFrame([y99(yearsu[i]) for i in np.arange(0,len(yearsu),1)])
    return(y99s)

def ann99(noaadf):
    y99 = pd.DataFrame([year99(i,noaadf) for i in np.arange(0,len(noaadf),)])
    return y99

year99s = pd.DataFrame([ann99(noaa[i]) for i in np.arange(0,len(noaa),1)])

#Daily 95%
#Daily 5%
#Median of each of above
#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Sat Feb 20 16:46:37 2021

@author: jtb188
"""
import pandas as pd
import numpy as np

aims = aims_data

for i in np.arange(0,len(aims),1):
    aims[i]['POINTS'] = aims[i]['LONGITUDE'].astype(str) + "_" + aims[i]['LATITUDE'].astype(str)

#Filter by year
for i in np.arange(0,len(aims),1):
    aims[i]['YEAR'] = aims[i]['TIMESTAMP'].dt.year

aims2014 = [None]*12
for i in np.arange(0,len(aims),1):
    aims2014[i] = aims[i][aims[i]['YEAR']==2014]
    print(i)
aims2014 =pd.concat(aims2014).reset_index(level=[0])
    
aims2015 = [None]*12
for i in np.arange(0,len(aims),1):
    aims2015[i] = aims[i][aims[i]['YEAR']==2015]
    print(i)
aims2015 = pd.concat(aims2015).reset_index(level=[0])

#Medians
aims_meds14 = aims2014.groupby('POINTS')['TEMP_C','LONGITUDE','LATITUDE','DEPTH'].median()
aims_meds14.to_csv('aims_medians2014.csv',index=False)

aims_meds15 = aims2015.groupby('POINTS')['TEMP_C','LONGITUDE','LATITUDE','DEPTH'].median()
aims_meds15.to_csv('aims_medians2015.csv',index=False)

#99th percentiles
aims_q9914 = aims2014.groupby('POINTS')['TEMP_C','LONGITUDE','LATITUDE','DEPTH'].quantile(q=0.99)
aims_q9914.to_csv('aims_q99_2014.csv',index=False)

aims_q9915 = aims2015.groupby('POINTS')['TEMP_C','LONGITUDE','LATITUDE','DEPTH'].quantile(q=0.99)
aims_q9915.to_csv('aims_q99_2015.csv',index=False)

#Standard deviation
a = {'TEMP_C': ['std'],'LONGITUDE':['median'],'LATITUDE':['median'],'DEPTH':['median']}
aims_sd14 = aims2014.groupby('POINTS').agg(a)
aims_sd14.to_csv('aims_sd_2014.csv',index=False)

aims_sd15 = aims2015.groupby('POINTS').agg(a)
aims_sd15.to_csv('aims_sd_2015.csv',index=False)
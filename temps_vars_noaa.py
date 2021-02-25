#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Mon Feb 15 13:10:24 2021

@author: jtb188
"""
import pandas as pd
import numpy as np

noaa = [noaa_amsam,noaa_batangas,noaa_biscayne,noaa_hawaii,noaa_keys,noaa_marianas,noaa_pria,noaa_pr,noaa_stcroix,noaa_stjohn,noaa_stthomas,noaa_timorleste,noaa_wake]

for i in np.arange(0,len(noaa),1):
    noaa[i]['POINTS'] = noaa[i]['LONGITUDE'].astype(str) + "_" + noaa[i]['LATITUDE'].astype(str)

#Global median
meds = pd.concat([noaa[i].groupby('POINTS')['TEMP_C','LONGITUDE','LATITUDE'].median() for i in np.arange(0,len(noaa),1)])
meds.to_csv('noaa_medians.csv',index=False)

#Global 99th percentile
q99 = pd.concat([noaa[i].groupby('POINTS')['TEMP_C','LONGITUDE','LATITUDE'].quantile(q=0.99) for i in np.arange(0,len(noaa),1)])
q99.to_csv('noaa_q99.csv',index=False)

#Global standard deviation
sd = pd.concat([noaa[i].groupby('POINTS')['TEMP_C','LONGITUDE','LATITUDE'].std() for i in np.arange(0,len(noaa),1)])

#Max - min by day
for i in np.arange(0,len(noaa),1):
    noaa[i]['DAYS'] = noaa[i]['TIMESTAMP'].dt.date
    
for i in np.arange(0,len(noaa),1):
    noaa[i]['dmin'] = noaa[i].groupby(['POINTS','DAYS']).TEMP_C.transform('min')
    noaa[i]['dmax'] = noaa[i].groupby(['POINTS','DAYS']).TEMP_C.transform('max')
    noaa[i]['DAILY_DIFF'] = noaa[i]['dmax'] - noaa[i]['dmin']
    noaa[i] = noaa[i].drop(columns=['dmin','dmax'])
    
#Filter by year
for i in np.arange(0,len(noaa),1):
    noaa[i]['YEAR'] = noaa[i]['TIMESTAMP'].dt.year

noaa2014 = [None]*13
for i in np.arange(0,len(noaa),1):
    noaa2014[i] = noaa[i][noaa[i]['YEAR']==2014]
    print(i)
    
noaa2015 = [None]*13
for i in np.arange(0,len(noaa),1):
    noaa2015[i] = noaa[i][noaa[i]['YEAR']==2015]
    print(i)
    
#Annual stats median

#Medians
meds14 = pd.concat([noaa2014[i].groupby('POINTS')['TEMP_C','LONGITUDE','LATITUDE','DEPTH'].median() for i in np.arange(0,len(noaa),1)],keys=['amsam','batangas','biscayne','hawaii','flkeys','marianas','pria','pr','stcroix','stjohn','stthomas','timorleste','wake'],axis=0).reset_index(level=[0])
meds14.to_csv('noaa_medians2014.csv',index=False)

meds15 = pd.concat([noaa2015[i].groupby('POINTS')['TEMP_C','LONGITUDE','LATITUDE','DEPTH'].median() for i in np.arange(0,len(noaa),1)],keys=['amsam','batangas','biscayne','hawaii','flkeys','marianas','pria','pr','stcroix','stjohn','stthomas','timorleste','wake'],axis=0).reset_index(level=[0])
meds15.to_csv('noaa_medians2015.csv',index=False)

#99th percentiles
q9914 = pd.concat([noaa2014[i].groupby('POINTS')['TEMP_C','LONGITUDE','LATITUDE','DEPTH'].quantile(q=0.99) for i in np.arange(0,len(noaa),1)],keys=['amsam','batangas','biscayne','hawaii','flkeys','marianas','pria','pr','stcroix','stjohn','stthomas','timorleste','wake'],axis=0).reset_index(level=[0])
q9914.to_csv('noaa_q99_2014.csv',index=False)

q9915 = pd.concat([noaa2014[i].groupby('POINTS')['TEMP_C','LONGITUDE','LATITUDE','DEPTH'].quantile(q=0.99) for i in np.arange(0,len(noaa),1)],keys=['amsam','batangas','biscayne','hawaii','flkeys','marianas','pria','pr','stcroix','stjohn','stthomas','timorleste','wake'],axis=0).reset_index(level=[0])
q9915.to_csv('noaa_q99_2015.csv',index=False)

#Standard deviation
a = {'TEMP_C': ['std'],'LONGITUDE':['median'],'LATITUDE':['median'],'DEPTH':['median']}
sd14 = pd.concat([noaa2014[i].groupby('POINTS').agg(a) for i in np.arange(0,len(noaa),1)],keys=['amsam','batangas','biscayne','hawaii','flkeys','marianas','pria','pr','stcroix','stjohn','stthomas','timorleste','wake'],axis=0).reset_index(level=[0])
sd14.to_csv('noaa_sd_2014.csv',index=False)

sd15 = pd.concat([noaa2015[i].groupby('POINTS').agg(a) for i in np.arange(0,len(noaa),1)],keys=['amsam','batangas','biscayne','hawaii','flkeys','marianas','pria','pr','stcroix','stjohn','stthomas','timorleste','wake'],axis=0).reset_index(level=[0])
sd15.to_csv('noaa_sd_2015.csv',index=False)
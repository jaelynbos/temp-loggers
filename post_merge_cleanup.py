#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Mon Jul  5 12:32:43 2021

@author: jtb188
"""
'''
Import required packages
'''
import pandas as pd
import numpy as np
import glob
import os

'''
Read in merged geomorphic and temperature data
'''
geopath = '/home/jtb188/Documents/allen_coral_atlas/agg_merge/'
geo_data = [pd.read_pickle(f) for f in sorted(glob.glob(geopath + "*pkl"))]

'''
Drop NAs from Florida and Caribbean
'''
geo_data[1]['TIMESTAMP'].isnull().value_counts()
geo_data[1] = geo_data[1].dropna(axis=0)
geo_data[6]['TIMESTAMP'].isnull().value_counts()
geo_data[6] = geo_data[6].dropna(axis=0)

'''
Downsample to every 30 minutes
'''
points_u = [None]*len(geo_data)
for i in np.arange(0,len(geo_data),1):
    points_u[i]= geo_data[i]['POINTS'].unique()

def thirtyminsample(region,region_points):
    region = region.set_index('TIMESTAMP')
    x = [region[region['POINTS'] == region_points[i]] for i in np.arange(0,len(region_points),1)]
    y = [x[i].resample('30T').first() for i in np.arange(0,len(x),1)]
    y = pd.concat(y)
    y = y.dropna(axis=0)
    return(y)

geo_downsample = [thirtyminsample(geo_data[j],points_u[j]) for j in np.arange(0,len(geo_data),1)]

'''
Pickle and export downsampled data
'''
os.chdir('/home/jtb188/Documents/')

coralsea_ds = geo_downsample[0]
coralsea_ds.to_pickle('coralsea_ds.pkl')

flkeys_ds = geo_downsample[1]
flkeys_ds.to_pickle('flkeys_ds.pkl')

gbr_ds = pd.concat([geo_downsample[2],geo_downsample[3],geo_downsample[4]])
gbr_ds.to_pickle('gbr_ds.pkl')

hawaii_ds = geo_downsample[5]
hawaii_ds.to_pickle('hawaii_ds.pkl')

secarib_ds = geo_downsample[6]
secarib_ds.to_pickle('secarib_ds.pkl')

solomon_ds = geo_downsample[7]
solomon_ds.to_pickle('solomon_ds.pkl')

swpacific_ds = geo_downsample[8]
swpacific_ds.to_pickle('swpacific_ds.pkl')

timorsea_ds = geo_downsample[9]
timorsea_ds.to_pickle('timorsea_ds.pkl')

westaus_ds = geo_downsample[10]
westaus_ds.to_pickle('westaus_ds.pkl')

westmicro_ds = geo_downsample[11]
westmicro_ds.to_pickle('westmicro_ds.pkl')

'''
Read in previously saved pickles (as necessary)
'''
geopath = '/home/jtb188/Documents/'
geo_ds = [pd.read_pickle(f) for f in sorted(glob.glob(geopath + "*pkl"))]

'''
Check for complete months (>27 days of observations per month)
'''
for i in np.arange(0,len(geo_ds),1):
    geo_ds[i]['TIMESTAMP'] = geo_ds[i].index
    
'Create date variable'
for i in np.arange(0,len(geo_ds),1):
    geo_ds[i]['DATE'] = geo_ds[i]['TIMESTAMP'].dt.date

for i in np.arange(0,len(geo_ds),1):
    geo_ds[i]['YRMONTHPOINT'] = geo_ds[i]['TIMESTAMP'].dt.year.astype('str') + '/' + geo_ds[i]['TIMESTAMP'].dt.month.astype('str') +'_' + geo_ds[i]['POINTS'].astype('str')
        
geo_agg = [None]*len(geo_ds)
for i in np.arange(0,len(geo_agg),1):
    geo_agg[i] = geo_ds[i].groupby(['DATE','POINTS']).first().reset_index()

monthn = [None]*len(geo_agg)
for i in np.arange(0,len(geo_agg),1):
    monthn[i] = geo_agg[i]['YRMONTHPOINT'].value_counts()
    monthn[i] = pd.DataFrame(monthn[i])
    monthn[i]['counts'] =monthn[i].values
    monthn[i]['YRMONTHPOINT'] =monthn[i].index

for i in np.arange(0,len(monthn),1):
    geo_ds[i] = pd.merge(geo_ds[i],monthn[i],how='left',on='YRMONTHPOINT')

'''
Keep only months with >25 days of observation at given point
'''

for i in np.arange(0,len(geo_ds),1):
    geo_ds[i] = geo_ds[i][geo_ds[i]['counts'] > 25]
    geo_ds[i] = geo_ds[i].drop(columns=['YRMONTHPOINT','counts'])
    
del geo_agg,monthn

'''
Keep only points with 12 months of complete data
'''
for i in np.arange(0,len(geo_ds),1):
    geo_ds[i]['MONTH'] = geo_ds[i]['TIMESTAMP'].dt.month

geo_agg = [None]*len(geo_ds)
for i in np.arange(0,len(geo_agg),1):
    geo_agg[i] = geo_ds[i].groupby(['POINTS','MONTH']).first().reset_index()

monthn = [None]*len(geo_agg)
for i in np.arange(0,len(geo_agg),1):
    monthn[i] = geo_agg[i]['POINTS'].value_counts()
    monthn[i] = pd.DataFrame(monthn[i])
    monthn[i]['counts'] =monthn[i].values
    monthn[i]['POINTS'] =monthn[i].index

for i in np.arange(0,len(monthn),1):
    geo_ds[i] = pd.merge(geo_ds[i],monthn[i],how='left',on='POINTS')
    
for i in np.arange(0,len(geo_ds),1):
    geo_ds[i] = geo_ds[i][geo_ds[i]['counts'] > 11]
    geo_ds[i] = geo_ds[i].drop(columns=['counts'])
'''
Rename class variable as RCLASS
'''
for i in np.arange(0,len(geo_ds),1):
    geo_ds[i]['RCLASS'] = geo_ds[i]['class']
    geo_ds[i] = geo_ds[i].drop(columns=['class'])

'''
Calculate daily temperature ranges
'''
for i in np.arange(0,len(geo_ds),1):
    geo_ds[i]['dmin'] = geo_ds[i].groupby(['POINTS','DATE']).TEMP_C.transform('min')
    geo_ds[i]['dmax'] = geo_ds[i].groupby(['POINTS','DATE']).TEMP_C.transform('max')
    geo_ds[i]['DAILY_DIFF'] = geo_ds[i]['dmax'] - geo_ds[i]['dmin']
    geo_ds[i] = geo_ds[i].drop(columns=['dmin','dmax'])
    
'''
Calculate max monthly means
'''
points_u = [None]*len(geo_ds)
for i in np.arange(0,len(geo_ds),1):
    points_u[i] = pd.Series(geo_ds[i]['POINTS'].unique())

a = {'DAILY_DIFF': ['median'],'TEMP_C': ['mean'],'LONGITUDE':['median'],'LATITUDE':['median'],'DEPTH':['median'],'RCLASS':['first'],'COORDS':['first']}
mms = [geo_ds[i].groupby(['MONTH','POINTS']).agg(a).reset_index() for i in np.arange(0,len(geo_ds),1)]

def sitesel(region,point):
    return(region.loc[region['POINTS']==point].reset_index())
    
def maxsel(site):
   return(site.loc[(site['TEMP_C']==site['TEMP_C'].max()).values])

def mmm_select(region,regional_points):
    return(pd.concat([maxsel(sitesel(region,regional_points[i])) for i in np.arange(0,len(regional_points),1)]).reset_index())

MMMs = [mmm_select(mms[i],points_u[i]) for i in np.arange(0,len(mms),1)]

for i in np.arange(0,len(MMMs),1):
    MMMs[i] = MMMs[i].drop(columns='level_0')
    MMMs[i] = MMMs[i].reset_index()
    MMMs[i].columns = ['level_0','index','MONTHS','POINTS','DAILY_DIFF','TEMP_C','LONGITUDE','LATITUDE','DEPTH','RCLASS','COORDS']
    MMMs[i] = pd.DataFrame(MMMs[i])

'''
Create single dataframe with MMMs for all regions
'''

MMMs_all = pd.concat(MMMs)

MMMs_all.to_csv('MMMs.csv')
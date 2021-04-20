#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Tue Mar 23 15:40:09 2021

@author: jtb188
"""
import numpy as np
import pandas as pd
import os

os.chdir('/home/jtb188/Documents/allen_coral_atlas')
hawaii_join = pd.read_csv('hawaii_join.csv')
marianas_join = pd.read_csv('marianas_join.csv')
flkeys_join = pd.read_csv('floridakeys_join.csv')
timorleste_join = pd.read_csv('timorleste_join.csv')

os.chdir('/home/jtb188/Documents/allen_coral_atlas/agg_merge')

solomon = pd.read_pickle('solomon.pkl')

#MMM and dailydiff from geomorphic merge sets
geo_merges = [solomon,timorsea,coralsea,westaus]

for i in np.arange(0,len(geo_merges),1):
    geo_merges[i]['TIMESTAMP'] = pd.to_datetime(geo_merges[i]['TIMESTAMP'])

for i in np.arange(0,len(geo_merges),1):
    geo_merges[i]['RCLASS'] = geo_merges[i]['class']
    
points_u = [None]*len(geo_merges)
for i in np.arange(0,len(geo_merges),1):
   points_u[i]= geo_merges[i]['POINTS'].unique()
#Daily diffs
for i in np.arange(0,len(geo_merges),1):
    geo_merges[i]['DAYS'] = geo_merges[i]['TIMESTAMP'].dt.date

for i in np.arange(0,len(geo_merges),1):
    geo_merges[i]['dmin'] = geo_merges[i].groupby(['POINTS','DAYS']).TEMP_C.transform('min')
    geo_merges[i]['dmax'] = geo_merges[i].groupby(['POINTS','DAYS']).TEMP_C.transform('max')
    geo_merges[i]['DAILY_DIFF'] = geo_merges[i]['dmax'] - geo_merges[i]['dmin']
    geo_merges[i] = geo_merges[i].drop(columns=['dmin','dmax'])
    
#Aggregate by month

for i in np.arange(0,len(geo_merges),1):
    geo_merges[i]['MONTH'] = geo_merges[i]['TIMESTAMP'].dt.month
    
a = {'DAILY_DIFF': ['mean'],'TEMP_C': ['mean'],'LONGITUDE':['median'],'LATITUDE':['median'],'DEPTH':['median'],'RCLASS':['first'],'COORDS':['first']}
mms = [geo_merges[i].groupby(['MONTH','POINTS']).agg(a).reset_index() for i in np.arange(0,len(geo_merges),1)]

def sitesel(region,point):
    return(region.loc[region['POINTS']==point].reset_index())
    
def maxsel(site):
   return(site.loc[(site['TEMP_C']==site['TEMP_C'].max()).values])

def mmm_select(region,regional_points):
    return(pd.concat([maxsel(sitesel(region,regional_points[i])) for i in np.arange(0,len(regional_points),1)]).reset_index())

MMMs = [mmm_select(geo_merges[i],points_u[i]) for i in np.arange(0,len(geo_merges),1)]
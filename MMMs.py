#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Tue Mar 23 15:40:09 2021

@author: jtb188
"""
import numpy as np
import pandas as pd

#MMM and dailydiff from geomorphic merge sets
geo_merges = geo_merges

points_u = [None]*len(geo_merges)
for i in np.arange(0,len(geo_merges),1):
   points_u[i]= geo_merges[i]['POINTS'].unique()
   
for i in np.arange(0,len(geo_merges),1):
    geo_merges[i]['MONTH'] = geo_merges[i]['TIMESTAMP'].dt.month
    
a = {'TEMP_C': ['mean'],'LONGITUDE':['median'],'LATITUDE':['median'],'DEPTH':['median']}
mms = [geo_merges[i].groupby(['MONTH','POINTS']).agg(a).reset_index() for i in np.arange(0,len(geo_merges),1)]

def sitesel(region,point):
    return(region.loc[region['POINTS']==point].reset_index())
    
def maxsel(site):
   return(site.loc[(site['TEMP_C']==site['TEMP_C'].max()).values])

def mmm_select(region,regional_points):
    return(pd.concat([maxsel(sitesel(region,regional_points[i])) for i in np.arange(0,len(regional_points),1)]).reset_index())

MMMs = [mmm_select(geo_merges[i],points_u[i]) for i in np.arange(0,len(geo_merges),1)]

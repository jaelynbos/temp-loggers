#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Mon Apr  5 14:44:50 2021

@author: jtb188
"""
import pandas as pd
import numpy as np
import glob

geopath = '/home/jtb188/Documents/allen_coral_atlas/agg_merge/'
geo_data = [pd.read_pickle(f) for f in sorted(glob.glob(geopath + "*pkl"))]

points_u = [None]*len(geo_data)

for i in np.arange(0,len(geo_data),1):
    points_u[i] = pd.Series(geo_data[i]['POINTS'].unique())

for i in np.arange(0,len(geo_data),1):
    geo_data[i]['YRMONTH'] = geo_data[i]['TIMESTAMP'].dt.year.astype('str') + '/' + geo_data[i]['TIMESTAMP'].dt.month.astype('str')
    
def monthsinsite(site):
    mo = site['YRMONTH'].unique()
    return([site[site['YRMONTH']==mo[i]] for i in np.arange(0,len(mo),1)])
        
def daysinmonth(site_month):
    x = len(site_month['TIMESTAMP'].dt.date.unique())
    if x > 27: 
        return (True)
    else:
        return (False)

def ntrue(point):
    y = pd.Series([daysinmonth(monthsinsite(point)[i]) for i in np.arange(0,len(monthsinsite(point)),1)])
    return(y.value_counts())

def complete_months(df,points_u):
    cm = [ntrue(df[df['POINTS'] == points_u[i]]) for i in np.arange(0,len(points_u),1)]
    return(cm)

dummy = [complete_months(geo_data[i],points_u[i]) for i in np.arange(0,len(geo_data),1)]
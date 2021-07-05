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
geo_downsample = [pd.read_pickle(f) for f in sorted(glob.glob(geopath + "*pkl"))]

#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Sun Aug 22 12:44:44 2021

@author: jtb188
"""

from shapely.geometry import Point
import numpy as np
import pandas as pd
import xarray as xr
import geopandas as gpd
import os
import glob

os.chdir('/home/jtb188/Documents/New_Caledonia/')

new_cal = [xr.open_dataset(j) for j in glob.glob("*.nc")]
newcal = [pd.DataFrame(columns = ['LATITUDE','LONGITUDE','DEPTH','TEMP_C','TIMESTAMP'])]*len(new_cal)

for i in np.arange(0,len(new_cal),1):
    newcal[i]['TEMP_C'] = pd.Series(new_cal[i]['TEMP'][:,0].values)
    newcal[i]['TIMESTAMP'] = pd.Series(new_cal[i]['TEMP']['TIME'].values)
    newcal[i]['TIMESTAMP'] = pd.to_datetime(newcal[i]['TIMESTAMP'],format='%Y-%m-%d %H:%M:%S')
    newcal[i]['LATITUDE'] = new_cal[i]['LATITUDE'].item()
    newcal[i]['LONGITUDE'] = new_cal[i]['LONGITUDE'].item()
    newcal[i]['DEPTH'] = new_cal[i]['DEPTH'].item()
    newcal[i] = newcal[i][newcal[i]['TIMESTAMP'] > '2002-06-01 00:00:00']
    
newcal = pd.concat(newcal)

def make_point(df):
    df['POINTS'] = df['LONGITUDE'].astype(str) + "_" + df['LATITUDE'].astype(str)
    df['COORDS'] = df[['LONGITUDE','LATITUDE']].values.tolist()
    df['COORDS'] = df['COORDS'].apply(Point)
    df_gpd = gpd.GeoDataFrame(df,geometry='COORDS')
    return(df_gpd)

def remerge(df_joined,dat):
    df_joined['POINTS'] = df_joined['LONGITUDE'].astype(str) + "_" + df_joined['LATITUDE'].astype(str)
    return(pd.merge(dat,df_joined))

newcal = make_point(newcal)
di = {'LATITUDE':'first','LONGITUDE':'first'}
newcal_agg = gpd.GeoDataFrame(newcal.groupby('POINTS').agg(di)).reset_index()
newcal_agg = make_point(newcal_agg)

newcal_agg.crs = {'init':'EPSG:4326'}
swpacific_geo = gpd.read_file('/home/jtb188/Documents/allen_coral_atlas/swpacific_geo.gpkg',layer='Southwestern Pacific')

newcal = remerge(pd.DataFrame(gpd.sjoin(newcal_agg,swpacific_geo,how='inner',op='within')),newcal)

points_u= newcal['POINTS'].unique()

def thirtyminsample(region,region_points):
    region = region.set_index('TIMESTAMP')
    x = [region[region['POINTS'] == region_points[i]] for i in np.arange(0,len(region_points),1)]
    y = [x[i].resample('30T').first() for i in np.arange(0,len(x),1)]
    y = pd.concat(y)
    y = y.dropna(axis=0)
    return(y)

newcal_ds = thirtyminsample(newcal,points_u)

'''
Keep only months with >25 days of observation at given point
'''
newcal_ds['TIMESTAMP'] = newcal_ds.index
newcal_ds['DATE'] = newcal_ds['TIMESTAMP'].dt.date
newcal_ds['YRMONTHPOINT'] = newcal_ds['TIMESTAMP'].dt.year.astype('str') + '/' + newcal_ds['TIMESTAMP'].dt.month.astype('str') +'_' + newcal_ds['POINTS'].astype('str')

newcal_agg = newcal_ds.groupby(['DATE','POINTS']).first().reset_index()

monthn = pd.DataFrame(newcal_agg['YRMONTHPOINT'].value_counts())
monthn['counts'] =monthn.values
monthn['YRMONTHPOINT'] =monthn.index

newcal_ds = pd.merge(newcal_ds,monthn,how='left',on='YRMONTHPOINT')

newcal_ds = newcal_ds[newcal_ds['counts'] > 25]
newcal_ds = newcal_ds.drop(columns=['YRMONTHPOINT','counts'])

'''
Keep only points with 12 months of complete data
'''
newcal_ds['MONTH'] = newcal_ds['TIMESTAMP'].dt.month
newcal_agg= newcal_ds.groupby(['POINTS','MONTH']).first().reset_index()
monthn = newcal_agg['POINTS'].value_counts()
monthn = pd.DataFrame(monthn)
monthn['counts'] =monthn.values
monthn['POINTS'] =monthn.index

newcal_ds = pd.merge(newcal_ds,monthn, how='left',on='POINTS')
    
newcal_ds = newcal_ds[newcal_ds['counts'] > 11]
newcal_ds = newcal_ds.drop(columns=['counts'])

'''
Rename geomorphic class variable
'''

newcal_ds['RCLASS'] = newcal_ds['class']
newcal_ds = newcal_ds.drop(columns=['class'])

'''
Calculate daily temperature ranges
'''
newcal_ds['dmin'] = newcal_ds.groupby(['POINTS','DATE']).TEMP_C.transform('min')
newcal_ds['dmax'] = newcal_ds.groupby(['POINTS','DATE']).TEMP_C.transform('max')
newcal_ds['DAILY_DIFF'] = newcal_ds['dmax'] - newcal_ds['dmin']
newcal_ds = newcal_ds.drop(columns=['dmin','dmax'])
    
'''
Calculate max monthly means
'''
points_u = pd.Series(newcal_ds['POINTS'].unique())

a = {'DAILY_DIFF': ['median'],'TEMP_C': ['mean'],'LONGITUDE':['median'],'LATITUDE':['median'],'DEPTH':['median'],'RCLASS':['first'],'COORDS':['first']}
mms = newcal_ds.groupby(['MONTH','POINTS']).agg(a).reset_index() 

def sitesel(region,point):
    return(region.loc[region['POINTS']==point].reset_index())
    
def maxsel(site):
   return(site.loc[(site['TEMP_C']==site['TEMP_C'].max()).values])

def mmm_select(region,regional_points):
    return(pd.concat([maxsel(sitesel(region,regional_points[i])) for i in np.arange(0,len(regional_points),1)]).reset_index())

MMMs = mmm_select(mms,points_u)
MMMs = MMMs.drop(columns='level_0')
MMMs = MMMs.reset_index()
MMMs.columns = ['level_0','index','MONTH','POINTS','DAILY_DIFF','TEMP_C','LONGITUDE','LATITUDE','DEPTH','RCLASS','COORDS']

MMMs.to_csv('newcal_MMMs.csv')

sst_points = MMMs.copy()
newcal_pts = MMMs
newcal_pts = newcal_pts.drop(columns=['level_0','DAILY_DIFF','TEMP_C','DEPTH','RCLASS'])
    
starts = newcal_ds.groupby(['POINTS'])['TIMESTAMP'].min().to_frame().reset_index() 
starts.columns = ['POINTS','START_TIME']

ends = newcal_ds.groupby(['POINTS'])['TIMESTAMP'].max().to_frame().reset_index() 
ends.columns = ['POINTS','END_TIME']

newcal_pts  = pd.merge(newcal_pts,starts)
newcal_pts  = pd.merge(newcal_pts,ends)
    
newcal_pts.to_csv('newcal_pts.csv')

"""
CALCULATE SSTs FROM NASA SERVER
"""
'''
Import SST data (generated on NASA server)
'''

newcal_sst = pd.read_csv('newcal_sst.csv')

newcal_pts = newcal_pts.reset_index()
newcal_sst = newcal_sst.reset_index()

newcal_sst.columns = ['a','b','SST_MMM']
newcal_sst = newcal_sst.drop(columns = ['a','b'])
newcal_sst['SST_MMM'] = newcal_sst['SST_MMM'] - 273

newcal_pts = newcal_pts.drop(columns=['level_0','index','MONTH','START_TIME','END_TIME','LATITUDE','LONGITUDE','COORDS'])
newcal_pts['SST_MMM'] = newcal_sst['SST_MMM'] 

newcal_sst_merge = pd.merge(newcal_ds,newcal_pts,how='left',on='POINTS')

newcal_sst_merge.to_csv('newcal_sst_merge.csv')

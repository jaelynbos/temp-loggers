#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Tue Oct 12 12:43:25 2021

@author: jtb188
"""
import numpy as np
import pandas as pd
import xarray as xr
import geopandas as gpd
from shapely.geometry import Point

def make_point(df):
    df['COORDS'] = df[['LONGITUDE','LATITUDE']].values.tolist()
    df['COORDS'] = df['COORDS'].apply(Point)
    df_gpd = gpd.GeoDataFrame(df,geometry='COORDS')
    return(df_gpd)

def agg_and_merge(region,geo):
    di = {'LATITUDE':['first'],'LONGITUDE':'first'}
    reg_agg = region.groupby('ID').agg(di).reset_index()
    reg_agg.columns = ['ID','LATITUDE','LONGITUDE']
    reg_agg = make_point(reg_agg)
    reg_agg.crs = geo.crs
    reg = pd.DataFrame(gpd.sjoin(reg_agg,geo,how='inner',op='within'))
    region = pd.merge(reg,region,on='ID') 
    region['RCLASS'] = region['class']
    region['LONGITUDE'] = region['LONGITUDE_x']
    region['LATITUDE'] = region['LATITUDE_x']
    region = region.drop(columns=['LONGITUDE_x','LATITUDE_x','LONGITUDE_y','LATITUDE_y','index_right','class'])
    return(region)

def thirtyminsample(region,region_points):
    region = region.set_index('DATETIME')
    x = [region[region['ID'] == region_points[i]] for i in np.arange(0,len(region_points),1)]
    y = [x[i].resample('30T').first() for i in np.arange(0,len(x),1)]
    y = pd.concat(y)
    y = y.dropna(axis=0)
    return(y)

def downsampler (region):
    points_u = region['ID'].unique()
    region_ds = thirtyminsample(region,points_u)
    region_ds['DATETIME'] = region_ds.index
    region_ds['DATE'] = region_ds['DATETIME'].dt.date
    region_ds['YRMONTHPOINT'] = region_ds['DATETIME'].dt.year.astype('str') + '/' + region_ds['DATETIME'].dt.month.astype('str') +'_' + region_ds['ID'].astype('str')
    return(region_ds)

def sitesel(region,point):
    return(region.loc[region['ID']==point].reset_index())
    
def maxsel(site):
   return(site.loc[(site['TEMP_C']==site['TEMP_C'].max()).values])

def mmm_select(region,regional_points):
    return(pd.concat([maxsel(sitesel(region,regional_points[i])) for i in np.arange(0,len(regional_points),1)]).reset_index())

def quantile95(ser):
    return(ser.quantile(q=0.95))
    
def quantile98(ser):
    return(ser.quantile(q=0.98))
    
def prep_data(region_ds):
        region_ds['q95'] = region_ds['TEMP_C']
        region_ds['q98'] = region_ds['TEMP_C']
        region_ds['dmin'] = region_ds.groupby(['ID','DATE']).TEMP_C.transform('min')
        region_ds['dmax'] = region_ds.groupby(['ID','DATE']).TEMP_C.transform('max')
        region_ds['DAILY_DIFF'] = region_ds['dmax'] - region_ds['dmin']
        region_ds = region_ds.drop(columns=['dmin','dmax'])
        points_u= pd.Series(region_ds['ID'].unique())
        a = {'q95':[quantile95],'q98':[quantile98],'DAILY_DIFF': ['median'],'TEMP_C': ['mean'],'LONGITUDE':['median'],'LATITUDE':['median'],'RCLASS':['first'],'COORDS':['first']}
        mms = region_ds.groupby(['MONTH','ID']).agg(a).reset_index() 
        MMMs = mmm_select(mms,points_u)
        return(MMMs)

def prep_data_more(region_ds,region_mmms):    
        starts = region_ds.groupby(['ID'])['DATETIME'].min().to_frame().reset_index()
        starts.columns = ['ID','START_TIME']
        ends = region_ds.groupby(['ID'])['DATETIME'].max().to_frame().reset_index()
        ends.columns = ['ID','END_TIME']
        pts = region_mmms.copy()
        pts = pd.merge(pts,starts)
        pts = pd.merge(pts,ends)
        pts = pts.reset_index()
        return(pts)
    
keepcols = ['LATITUDE','LONGITUDE','ID','DATETIME','TEMP_C']

'''
Points from Dongsha, South China Sea
'''
dongsha_xr = xr.open_dataset('/projects/f_mlp195/bos_microclimate/loggers_testing/dongsha_temps.netcdf')

dongsha = pd.DataFrame()
dongsha['LATITUDE'] = dongsha_xr['lat']
dongsha['LONGITUDE'] = dongsha_xr['lon']
dongsha['ISO_DateTime_UTC'] = dongsha_xr['ISO_DateTime_UTC']
dongsha['time'] = dongsha_xr['time']
dongsha['month'] = dongsha_xr['mon']
dongsha['year'] = dongsha_xr['year']
dongsha['day'] = dongsha_xr['day']
dongsha['TEMP_C'] = dongsha_xr['temp']
dongsha['ID'] = dongsha_xr['location']

'''
Convert variables to numeric in a very hackish way
Original data type listed as 'bytes4104'
'''

def toNum (series):
    series = series.apply(str)
    series = series.str.strip('b')
    series = series.str.strip("'")
    series = series.apply(float)
    return(series)
    
dongsha['LATITUDE'] = toNum(dongsha['LATITUDE'])
dongsha['LONGITUDE'] = toNum(dongsha['LONGITUDE'])

dongsha['day'] = toNum(dongsha['day'])
dongsha['month'] = toNum(dongsha['month'])
dongsha['year'] = dongsha['year'].apply(int)
dongsha['month'] = dongsha['month'].apply(int).apply(str).str.zfill(2)
dongsha['day'] = dongsha['day'].apply(int).apply(str).str.zfill(2)
dongsha['time'] = dongsha['time'].apply(int).apply(str).str.zfill(4)
dongsha['TEMP_C'] = toNum(dongsha['TEMP_C'])
'''
Construct a datetime variable from pieces because nothing is easy in this life
'''
times = dongsha['time'].apply(list)
minutes = dongsha['time'].str[2] + dongsha['time'].str[3]
hours = dongsha['time'].str[0] + dongsha['time'].str[1]
dongsha['DATE'] = dongsha['year'].apply(str) + "-" + dongsha['month'].apply(str) + "-" + dongsha['day'].apply(str)
dongsha['TIME'] = hours + ":" + minutes + ":" + '00'
dongsha['DATETIME'] = pd.to_datetime((dongsha['DATE'] + " " + dongsha['time']),format='%Y-%m-%d %H:%M:%S')

dongsha = dongsha[keepcols]
'''
Merge with geomorphic data
'''
dongsha_geo = gpd.read_file('/projects/f_mlp195/bos_microclimate/geomorphology/South-China-Sea.gpkg',layer='South China Sea')
dongsha = agg_and_merge(dongsha,dongsha_geo)

'''
Merge and temporal filter
'''
dongsha = downsampler(dongsha)
dongsha['MONTH'] =dongsha['DATETIME'].dt.month
d_agg = dongsha.groupby(['ID','MONTH']).first().reset_index()

'''
Remove points with <12 months
'''
monthn= pd.DataFrame(d_agg['ID'].value_counts())
monthn['counts'] =monthn.values
monthn['ID'] =monthn.index
monthn = monthn.reset_index()

dongsha = pd.merge(dongsha,monthn,how='left',on='ID')
dongsha = dongsha[dongsha['counts'] > 11]
dongsha = dongsha.reset_index(drop=True)

dongsha_mmms = prep_data(dongsha)
dongsha_mmms.columns = ['level_0','index','MONTH','ID','q95','q98','DAILY_DIFF','TEMP_C','LONGITUDE','LATITUDE','RCLASS','COORDS']
dongsha_mmms = dongsha_mmms.drop(columns=['level_0','index'])

dongsha = prep_data_more(dongsha,dongsha_mmms)
outfile = open('/home/jtb188/dongsha.csv','wb')
dongsha.to_csv(outfile)
outfile.close()

#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Fri Jan 20 14:29:29 2023

@author: jtb188
"""
import numpy as np
import pandas as pd
import geopandas as gpd
import xarray as xr
from shapely.geometry import Point
from scipy.io import loadmat

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

pipa_xr = xr.open_dataset('/projects/f_mlp195/bos_microclimate/loggers_testing/PIPA_temperature.netcdf')

pipa = pd.DataFrame()
pipa['LATITUDE'] = pipa_xr['Latitude']
pipa['LONGITUDE'] = pipa_xr['Longitude']
pipa['TEMP_C'] = pipa_xr['Temperature']
pipa['Date'] = pipa_xr['Date']
pipa['Hour'] = pipa_xr['Hour']
pipa['island'] = pipa_xr['Island']

def toNum (series):
    series = series.apply(str)
    series = series.str.strip('b')
    series = series.str.strip("'")
    series = series.apply(float)
    return(series)

pipa['LATITUDE'] = toNum(pipa['LATITUDE'])
pipa['LONGITUDE'] = toNum(pipa['LONGITUDE'])
pipa['TEMP_C'] = toNum(pipa['TEMP_C'])

pipa['Date'] = pipa['Date'].apply(str)
pipa['Hour'] = pipa['Hour'].apply(str)

pipa['Date'] = pipa['Date'].str.strip('b')
pipa['Date'] = pipa['Date'].str.strip("'")

pipa['Hour'] = pipa['Hour'].str.strip('b')
pipa['Hour'] = pipa['Hour'].str.strip("'")

pipa['Datetime'] = pipa['Date'] + ':' + pipa['Hour']
pipa['DATETIME']  = pd.to_datetime(pipa['Datetime'],format='%Y-%m-%d:%H')
pipa['ID'] = pd.Series(pipa['LATITUDE'].apply(str) + pipa['LONGITUDE'].apply(str)).apply(str)
pipa = pipa[keepcols]
'''
Add geomorphology
'''
pipa_geo = gpd.read_file('/projects/f_mlp195/bos_microclimate/geomorphology/Eastern-Micronesia_E.gpkg')
pipa_geo.columns = ['class','geometry']

pipa = agg_and_merge(pipa,pipa_geo)

'''
Merge and temporal filter
'''
pipa = downsampler(pipa)
pipa['MONTH'] =pipa['DATETIME'].dt.month
d_agg = pipa.groupby(['ID','MONTH']).first().reset_index()

'''
Remove points with <12 months
'''
monthn= pd.DataFrame(d_agg['ID'].value_counts())
monthn['counts'] =monthn.values
monthn['ID'] =monthn.index
monthn = monthn.reset_index()

pipa = pd.merge(pipa,monthn,how='left',on='ID')
pipa = pipa[pipa['counts'] > 11]
pipa = pipa.reset_index(drop=True)

pipa_mmms = prep_data(pipa)
pipa_mmms.columns = ['level_0','index','MONTH','ID','q95','q98','DAILY_DIFF','TEMP_C','LONGITUDE','LATITUDE','RCLASS','COORDS']
pipa_mmms = pipa_mmms.drop(columns=['level_0','index'])

pipa = prep_data_more(pipa,pipa_mmms)
outfile = open('/home/jtb188/pipa.csv','wb')
pipa.to_csv(outfile)
outfile.close()
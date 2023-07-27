#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Wed Feb 10 09:05:37 2021

@author: jtb188
"""
import pandas as pd
import numpy as np
from datetime import datetime
import fiona
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

'''
Import data as Pandas dataframe and move relevant columns to new pandas dataframe
'''
aims_data = pd.read_csv('/projects/f_mlp195/bos_microclimate/loggers_training/aims-logger-data.csv')

aims_data.columns

aims = pd.DataFrame()
aims['LATITUDE'] = aims_data['lat']
aims['LONGITUDE'] = aims_data['lon']
aims['DATETIME'] = pd.to_datetime(aims_data['time'],infer_datetime_format=True)
aims['TEMP_C'] = aims_data['qc_val']
aims['ID'] = aims_data['subsite_id']


'''
Filter out rows where time = NaN
'''
aims = aims[aims['TEMP_C']>0]

'''
Filter out rows before the year 2003 (to agree with MUR SST data). Timezone is incorrect but matches input data
'''
aims = aims[aims['DATETIME']>pd.to_datetime('2003-01-01T00:00:00+00',infer_datetime_format=True)]

'''
Match to geomorphic data
'''
coral_geo = gpd.read_file('/projects/f_mlp195/bos_microclimate/geomorphology/Coral-Sea.gpkg')
westaus_geo = gpd.read_file('/projects/f_mlp195/bos_microclimate/geomorphology/Western-Australia.gpkg')
subeast_geo = gpd.read_file('/projects/f_mlp195/bos_microclimate/geomorphology/Subtropical-Eastern-Australia.gpkg')
timor_geo = gpd.read_file('/projects/f_mlp195/bos_microclimate/geomorphology/Timor-Arafura-Seas.gpkg')
enewguinea_geo = gpd.read_file('/projects/f_mlp195/bos_microclimate/geomorphology/Eastern-Papua-New-Guinea.gpkg')
gbr_geo = gpd.read_file('/projects/f_mlp195/bos_microclimate/geomorphology/Great-Barrier-Reef-and-Torres-Strait.gpkg')

coralsea = agg_and_merge(aims,coral_geo)
subeast = agg_and_merge(aims,subeast_geo)
westaus = agg_and_merge(aims,westaus_geo)
timor = agg_and_merge(aims,timor_geo)
gbr = agg_and_merge(aims,gbr_geo)

aims2 = pd.concat([coralsea,subeast,westaus,timor,gbr])
'''
Downsample to every 30 minutes for each point
'''
aims2 = downsampler(aims2)

aims2['MONTH'] =aims2['DATETIME'].dt.month
d_agg = aims2.groupby(['ID','MONTH']).first().reset_index()

'''
Remove points with <12 months
'''
monthn= pd.DataFrame(d_agg['ID'].value_counts())
monthn['counts'] =monthn.values
monthn['ID'] =monthn.index
monthn = monthn.reset_index()

aims2 = pd.merge(aims2,monthn,how='left',on='ID')
aims2 = aims2[aims2['counts'] > 11]
aims2 = aims2.reset_index(drop=True)

'''
Calculate MMMs
'''
aims_mmms = prep_data(aims2)
aims_mmms.columns = ['level_0','index','MONTH','ID','q95','q98','DAILY_DIFF','TEMP_C','LONGITUDE','LATITUDE','RCLASS','COORDS']
aims_mmms = aims_mmms.drop(columns=['level_0','index'])

aims = prep_data_more(aims2,aims_mmms)

outfile = open('/home/jtb188/aims.csv','wb')
aims.to_csv(outfile)
outfile.close()
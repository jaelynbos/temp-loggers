#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Mon Mar 20 13:50:59 2023

@author: jtb188
"""
'''
Processes data from ReefTEMPS - Reseau d'observation des eaux cotieres du pacifique insulaire
'''
import glob
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

path = '/projects/f_mlp195/bos_microclimate/loggers_testing/Newcal_loggers1/reeftemps_nc_data_20220317/'

reeftemps_all = [xr.open_dataset(f) for f in glob.glob(path+ "*.nc")]

reeftemps_pd = []
for i in range(0,len(reeftemps_all)):
    dat = pd.DataFrame()
    dat['TEMP_C'] = reeftemps_all[i]['TEMP'].to_series()
    dat['LATITUDE'] = reeftemps_all[i]['LATITUDE'].values[0]
    dat['LONGITUDE'] = reeftemps_all[i]['LONGITUDE'].values[0]
    dat['ID'] = reeftemps_all[i].platform_code
    dat = dat.reset_index()
    print(i)
    reeftemps_pd.append(dat)
    
reeftemps = pd.concat(reeftemps_pd).reset_index(drop=True)
reeftemps = reeftemps.rename(columns={'TIME':'DATETIME'})

'''
Geomorphology
'''
emicro_geo = gpd.read_file('/projects/f_mlp195/bos_microclimate/geomorphology/Eastern-Micronesia_W.gpkg')
emicro_geo = emicro_geo.rename(columns={'rclass':'class'})
emicro = agg_and_merge(reeftemps,emicro_geo)

cs_pacific_geo = gpd.read_file('/projects/f_mlp195/bos_microclimate/geomorphology/Central-South-Pacific.gpkg')
cs_pacific = agg_and_merge(reeftemps,cs_pacific_geo)

png_geo = gpd.read_file('/projects/f_mlp195/bos_microclimate/geomorphology/Eastern-Papua-New-Guinea.gpkg')
png = agg_and_merge(reeftemps,png_geo)

swpacific_w_geo = gpd.read_file('/projects/f_mlp195/bos_microclimate/geomorphology/Southwestern-Pacific_W.gpkg')
swpacific_w_geo = swpacific_w_geo.rename(columns={'rclass':'class'})
swpacific_w = agg_and_merge(reeftemps,swpacific_w_geo)

swpacific_e_geo = gpd.read_file('/projects/f_mlp195/bos_microclimate/geomorphology/Southwestern-Pacific_E.gpkg')
swpacific_e_geo = swpacific_e_geo.rename(columns={'rclass':'class'})
swpacific_e = agg_and_merge(reeftemps,swpacific_e_geo)

wmicro_geo = gpd.read_file('/projects/f_mlp195/bos_microclimate/geomorphology/Western-Micronesia.gpkg')
wmicro = agg_and_merge(reeftemps,wmicro_geo)

reeftemps = downsampler(pd.concat([emicro,cs_pacific,png,swpacific_w,swpacific_e,wmicro]).reset_index(drop=True))

'''
Remove data from before 2003
'''
reeftemps = reeftemps[reeftemps['DATETIME'] > pd.to_datetime('2003-01-01 00:00:00')]
reeftemps = reeftemps.reset_index(drop=True)

reeftemps['MONTH'] =reeftemps['DATETIME'].dt.month
d_agg = reeftemps.groupby(['ID','MONTH']).first().reset_index()

monthn= pd.DataFrame(d_agg['ID'].value_counts())
monthn['counts'] =monthn.values
monthn['ID'] =monthn.index
monthn = monthn.reset_index()

reeftemps = pd.merge(reeftemps,monthn,how='left',on='ID')
reeftemps = reeftemps[reeftemps['counts'] > 11]
reeftemps = reeftemps.reset_index(drop=True)

rt_mmms = prep_data(reeftemps)
rt_mmms.columns = ['level_0','index','MONTH','ID','q95','q98','DAILY_DIFF','TEMP_C','LONGITUDE','LATITUDE','RCLASS','COORDS']
rt_mmms = rt_mmms.drop(columns=['level_0','index'])
rtemps = prep_data_more(reeftemps,rt_mmms)

outfile = open('/home/jtb188/reeftemps.csv','wb')
rtemps.to_csv(outfile)
outfile.close()
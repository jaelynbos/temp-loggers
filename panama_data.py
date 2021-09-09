#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Mon Aug  2 11:06:02 2021

@author: jtb188
"""
import numpy as np
import pandas as pd
import geopandas as gpd
import fiona
from shapely.geometry import Point

'Bahia las minas'
blm = pd.read_csv('/home/jtb188/Documents/Panama/BahiaLasMinas/Bahia Las Minas_03m_wt.csv')
blm['TEMP_C'] = blm['raw']
blm['TIMESTAMP'] = pd.to_datetime(blm['datetime'])
blm['DEPTH'] = 3
blm['LATITUDE'] = 9.397833
blm['LONGITUDE'] = -79.82295

'Bocas platform'
bp = pd.read_csv('/home/jtb188/Documents/Panama/BocasPlatform/bocas_tower_wt_elect.csv')
bp['TEMP_C'] = bp['raw']
bp['TIMESTAMP'] = pd.to_datetime(bp['datetime'])
bp['DEPTH'] = 2
bp['LATITUDE'] = 9.35078
bp['LONGITUDE'] = -82.257686

'Galeta channel'
gc = pd.read_csv('/home/jtb188/Documents/Panama/Galeta_Channel_wt/Galeta Channel_03m_wt.csv')
gc['TEMP_C'] = gc['raw']
gc['TIMESTAMP'] = pd.to_datetime(gc['datetime'])
gc['DEPTH'] = 3
gc['LATITUDE'] = 9.401748
gc['LONGITUDE'] = -79.860455

'Galeta downstream'
gd = pd.read_csv('/home/jtb188/Documents/Panama/Galeta_Downstream_wt/Galeta Downstream_wt.csv')
gd['TEMP_C'] =gd['raw']
gd['TIMESTAMP'] = pd.to_datetime(gd['datetime'])
gd['DEPTH'] = 0.1
gd['LATITUDE'] = 9.402528
gd['LONGITUDE'] = -79.860639

'Galeta upstream'
gu = pd.read_csv('/home/jtb188/Documents/Panama/Galeta_Upstream_wt/Galeta Upstream_wt.csv')
gu['TEMP_C'] = gu['raw']
gu['TIMESTAMP'] = pd.to_datetime(gu['datetime'])
gu['DEPTH'] = 0.1
gu['LATITUDE'] = 9.403361
gu['LONGITUDE'] = -79.860361

'Isla Grande'
ig = pd.read_csv('/home/jtb188/Documents/Panama/Isla Grande_wt/Isla Grande_03m_wt.csv')
ig['TEMP_C'] = ig['raw']
ig['TIMESTAMP'] = pd.to_datetime(ig['datetime'])
ig['DEPTH'] = 3
ig['LATITUDE'] = 9.621389
ig['LONGITUDE'] = -79.565556

'Isla Cayo Agua'
ica = pd.read_csv('/home/jtb188/Documents/Panama/Isla_CayoAgua_wt/Isla Cayo Agua_03m_wt.csv')
ica['TEMP_C'] = ica['raw']
ica['TIMESTAMP'] = pd.to_datetime(ica['datetime'])
ica['DEPTH'] = 5.2
ica['LATITUDE'] = 9.133611
ica['LONGITUDE'] = -82.040278

'Isla Colon Mangrove Mud'
icm = pd.read_csv('/home/jtb188/Documents/Panama/Isla_Colon_MangroveMud_wt/Isla_Colon_MangroveMud_wt.csv')
icm['TEMP_C'] = icm['raw']
icm['TIMESTAMP'] = pd.to_datetime(icm['datetime'])
icm['DEPTH'] = 0
icm['LATITUDE'] = 7.431944
icm['LONGITUDE'] = -81.858056

'Isla Colon Seagrass'
ics = pd.read_csv('/home/jtb188/Documents/Panama/Isla_Colon_Seagrass_wt/Isla Colon-Seagrass_02m_wt.csv')
ics['TEMP_C'] = ics['raw']
ics['TIMESTAMP'] = pd.to_datetime(ics['datetime'])
ics['DEPTH'] = 2
ics['LATITUDE'] = 9.351694
ics['LONGITUDE'] = -82.257806

'Isla Colon'
ic = pd.read_csv('/home/jtb188/Documents/Panama/Isla_Colon_wt/Isla Colon_20m_wt.csv')
ic['TEMP_C'] = ic['raw']
ic['TIMESTAMP'] = pd.to_datetime(ic['datetime'])
ic['DEPTH'] = 18
ic['LATITUDE'] = 9.349417
ic['LONGITUDE'] = -82.26325

panama = pd.concat([blm,gc,gd,gu,ic,ica,icm,ics,ig])
panama = panama[panama['TEMP_C']>0]
fiona.listlayers('/home/jtb188/Documents/allen_coral_atlas/unused_geo/mesoamerica_geo.gpkg')
panama_geo = gpd.read_file('/home/jtb188/Documents/allen_coral_atlas/unused_geo/mesoamerica_geo.gpkg',layer='Mesoamerica')

def make_point(df):
    df['POINTS'] = df['LONGITUDE'].astype(str) + "_" + df['LATITUDE'].astype(str)
    df['COORDS'] = df[['LONGITUDE','LATITUDE']].values.tolist()
    df['COORDS'] = df['COORDS'].apply(Point)
    df_gpd = gpd.GeoDataFrame(df,geometry='COORDS')
    return(df_gpd)

panama = make_point(panama)
panama = panama.reset_index()

di = {'LATITUDE':['first'],'LONGITUDE':'first'}
panama_agg = panama.groupby('POINTS').agg(di).reset_index()
panama_agg.columns = ['POINTS','LATITUDE','LONGITUDE']
panama_agg = make_point(panama_agg)

panama_agg.crs = panama_geo.crs

pan = pd.DataFrame(gpd.sjoin(panama_agg,panama_geo,how='inner',op='within'))

panama = pd.merge(pan,panama,on='POINTS') 

panama['RCLASS'] = panama['class']
panama = panama.drop(columns=['LONGITUDE_y','LATITUDE_y','index_right','class','COORDS_y'])

points_u = panama['POINTS'].unique()

def thirtyminsample(region,region_points):
    region = region.set_index('TIMESTAMP')
    x = [region[region['POINTS'] == region_points[i]] for i in np.arange(0,len(region_points),1)]
    y = [x[i].resample('30T').first() for i in np.arange(0,len(x),1)]
    y = pd.concat(y)
    y = y.dropna(axis=0)
    return(y)

panama_ds = thirtyminsample(panama,points_u)
panama_ds['TIMESTAMP'] = panama_ds.index

panama_ds['DATE'] = panama_ds['TIMESTAMP'].dt.date
panama_ds['YRMONTHPOINT'] = panama_ds['TIMESTAMP'].dt.year.astype('str') + '/' + panama_ds['TIMESTAMP'].dt.month.astype('str') +'_' + panama_ds['POINTS'].astype('str')

pan_agg = panama_ds.groupby(['DATE','POINTS']).first().reset_index()

monthn = pd.DataFrame(pan_agg['YRMONTHPOINT'].value_counts())
monthn['counts'] =monthn.values
monthn['YRMONTHPOINT'] =monthn.index


panama_ds = pd.merge(panama_ds,monthn,how='left',on='YRMONTHPOINT')
panama_ds = panama_ds[panama_ds['counts'] > 25]
panama_ds = panama_ds.drop(columns=['YRMONTHPOINT','counts'])

del monthn

'''
drop data before June 1, 2002
'''
panama_ds = panama_ds[panama_ds['TIMESTAMP']> pd.to_datetime('2002-06-01 00:00:00' )]

panama_ds['MONTH'] = panama_ds['TIMESTAMP'].dt.month
pan_agg = panama_ds.groupby(['POINTS','MONTH']).first().reset_index()

monthn= pan_agg['POINTS'].value_counts()

'''
All points have >12 months; no need to filter
'''

'''
Calculate daily temperature ranges
'''

panama_ds['dmin'] = panama_ds.groupby(['POINTS','DATE']).TEMP_C.transform('min')
panama_ds['dmax'] = panama_ds.groupby(['POINTS','DATE']).TEMP_C.transform('max')
panama_ds['DAILY_DIFF'] = panama_ds['dmax'] - panama_ds['dmin']
panama_ds = panama_ds.drop(columns=['dmin','dmax'])

points_u= pd.Series(panama_ds['POINTS'].unique())

panama_ds.columns = ['POINTS','LATITUDE','LONGITUDE','COORDS','index','datetime','date','wt','raw','chk_note','chk_fail','cid','TEMP_C','DEPTH','COORDS_y','RCLASS','TIMESTAMP','DATE','MONTH','DAILY_DIFF']
a = {'DAILY_DIFF': ['median'],'TEMP_C': ['mean'],'LONGITUDE':['median'],'LATITUDE':['median'],'DEPTH':['median'],'RCLASS':['first'],'COORDS':['first']}
mms = panama_ds.groupby(['MONTH','POINTS']).agg(a).reset_index() 

def sitesel(region,point):
    return(region.loc[region['POINTS']==point].reset_index())
    
def maxsel(site):
   return(site.loc[(site['TEMP_C']==site['TEMP_C'].max()).values])

def mmm_select(region,regional_points):
    return(pd.concat([maxsel(sitesel(region,regional_points[i])) for i in np.arange(0,len(regional_points),1)]).reset_index())

MMMs = mmm_select(mms,points_u)

MMMs.columns = ['level_0','index','MONTH','POINTS','DAILY_DIFF','TEMP_C','LONGITUDE','LATITUDE','DEPTH','RCLASS','COORDS']

starts = panama_ds.groupby(['POINTS'])['TIMESTAMP'].min().to_frame().reset_index()
starts.columns = ['POINTS','START_TIME']

ends = panama_ds.groupby(['POINTS'])['TIMESTAMP'].max().to_frame().reset_index()
ends.columns = ['POINTS','END_TIME']

panama_points = MMMs.copy()
panama_points = pd.merge(panama_points,starts)
panama_points = pd.merge(panama_points,ends)
panama_points = panama_points.drop(columns = ['level_0','Unnamed: 0'])
panama_points = panama_points.reset_index()

panama_points.to_csv('panama_points.csv')

'''
Add SST MMMs from Pangeo server here!
'''
panama_points = pd.read_csv('/home/jtb188/Documents/Panama/panama_points.csv')
'''
Import SST data (generated on NASA Pangeo server)
'''
pan_sst = pd.read_csv('/home/jtb188/Documents/Panama/panama_mmms.csv')
pan_sst.columns = ['na','SST_MMM']
pan_sst['SST_MMM'] = pan_sst['SST_MMM'] - 273
pan_sst = pan_sst.reset_index()

panama_merge = pd.merge(panama_points,pan_sst,left_index = True,right_index= True)
os.chdir('/home/jtb188/Documents/Panama/')
panama_merge.to_csv('panama_merge.csv')
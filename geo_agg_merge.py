#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Thu Mar 25 19:19:15 2021

@author: jtb188
"""

import os
import numpy as np
import fiona
import pandas as pd
import geopandas as gpd
from shapely.geometry import Point

os.chdir('/home/jtb188/Documents/allen_coral_atlas/')

def make_point(df):
    df['POINTS'] = df['LONGITUDE'].astype(str) + "_" + df['LATITUDE'].astype(str)
    df['COORDS'] = df[['LONGITUDE','LATITUDE']].values.tolist()
    df['COORDS'] = df['COORDS'].apply(Point)
    df_gpd = gpd.GeoDataFrame(df,geometry='COORDS')
    return(df_gpd)

def remerge(df_joined):
    df_joined['POINTS'] = df_joined['LONGITUDE'].astype(str) + "_" + df_joined['LATITUDE'].astype(str)
    x = [None]*len(aims)
    for i in np.arange(0,len(aims),1):
        x[i] = pd.merge(aims[i],df_joined)
    return(x)

#AIMS
aims = aims

#Aggregate by point
di = {'LATITUDE':['first'],'LONGITUDE':'first'}
aims_agg = [None]*len(aims)

for i in np.arange(0,len(aims),1):
    aims_agg[i] = aims[i].groupby('POINTS').agg(di).reset_index()
    
aims_agg = pd.concat(aims_agg)

#Makepoint
aims_agg = make_point(aims_agg)
aims_agg.columns = ['POINTS','LATITUDE','LONGITUDE','COORDS']
aims_agg.crs = {'init':'EPSG:4326'}

#Geomorphic read
fiona.listlayers('gbr_geo.gpkg')
gbr_geo = gpd.read_file('gbr_geo.gpkg',layer='Great Barrier Reef and Torres Strait')
timor_geo = gpd.read_file('timor_geo.geojson')
solomon_geo = gpd.read_file('solomon_geo.geojson')
fiona.listlayers('coralsea_geo.gpkg')
coral_geo = gpd.read_file('coralsea_geo.gpkg',layer='Coral Sea')
fiona.listlayers('westaustralia_geo.gpkg')
westaustralia_geo = gpd.read_file('westaustralia_geo.gpkg',layer='Western Australia')

#Spatial merge
gbr = remerge(pd.DataFrame(gpd.sjoin(aims_agg,gbr_geo,how='inner',op='within')))
timorsea = remerge(pd.DataFrame(gpd.sjoin(aims_agg,timor_geo,how='inner',op='within')))
solomon = remerge(pd.DataFrame(gpd.sjoin(aims_agg,solomon_geo,how='inner',op='within')))
coralsea = remerge(pd.DataFrame(gpd.sjoin(aims_agg,coral_geo,how='inner',op='within')))
westaus = remerge(pd.DataFrame(gpd.sjoin(aims_agg,westaustralia_geo,how='inner',op='within')))

del gbr_geo, timor_geo,solomon_geo,coral_geo,westaustralia_geo
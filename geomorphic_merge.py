#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Wed Mar  3 18:46:12 2021

@author: jtb188
"""
import os
import numpy as np
import fiona
import pandas as pd
import geopandas as gpd
from shapely.geometry import Point

os.chdir('/home/jtb188/Documents/allen_coral_atlas/')

keepcols = ['LATITUDE','LONGITUDE','DEPTH','TIMESTAMP','TEMP_C']

def make_point(df):
    df['POINTS'] = df['LONGITUDE'].astype(str) + "_" + df['LATITUDE'].astype(str)
    df['COORDS'] = df[['LONGITUDE','LATITUDE']].values.tolist()
    df['COORDS'] = df['COORDS'].apply(Point)
    df_gpd = gpd.GeoDataFrame(df,geometry='COORDS')
    return(df_gpd)

#Extract geomorphic values to points

#Hawaii
hawaii_geo = gpd.read_file('hawaii_geo.geojson')

#Read in Hawaii NOAA data using noaa_data script
hawaii_gpd = make_point(noaa_hawaii)

hawaii_geo.crs
hawaii_gpd.crs = {'init':'EPSG:4326'}

hawaii_join = gpd.sjoin(hawaii_gpd,hawaii_geo,how='inner',op='intersects')

hawaii_join['RCLASS'] =  hawaii_join['class']
hawaii_join = hawaii_join.drop(['class'],axis=1)
hawaii_join.to_csv('hawaii_join.csv',index=False)

#Marianas
marianas_geo = gpd.read_file('marianas_geo.geojson')

#Read in Marianas NOAA data using noaa_data script
marianas_gpd = make_point(noaa_marianas)
marianas_gpd.crs = {'init':'EPSG:4326'}
marianas_join = gpd.sjoin(marianas_gpd,marianas_geo,how='inner',op='intersects')

marianas_join['RCLASS'] =  marianas_join['class']
marianas_join = marianas_join.drop(['class'],axis=1)
marianas_join.to_csv('marianas_join.csv',index=False)

#Florida
florida_geo = gpd.read_file('flkeys_geo.geojson')

#Read in Florida NOAA data using noaa_data script
flkeys_gpd = make_point(noaa_keys)
flkeys_gpd.crs = {'init':'EPSG:4326'}
flkeys_join = gpd.sjoin(flkeys_gpd,florida_geo,how='inner',op='intersects')

flkeys_join['RCLASS'] =  flkeys_join['class']
flkeys_join = flkeys_join.drop(['class'],axis=1)
flkeys_join.to_csv('floridakeys_join.csv',index=False)

#Southeast Asia
fiona.listlayers('seasia_geo.gpkg')
seasia_geo = gpd.read_file('seasia_geo.gpkg',layer='Southeast Asian Archipelago')

timorleste_gpd = make_point(noaa_timorleste)
timorleste_gpd.crs = {'init':'EPSG:4326'}
timorleste_join = gpd.sjoin(timorleste_gpd,seasia_geo,how='inner',op='intersects')

timorleste_join['RCLASS'] = timorleste_join['class']
timorleste_join = timorleste_join.drop(['class'],axis=1)
timorleste_join.to_csv('timorleste_join.csv',index=False)

#Great Barrier Reef
fiona.listlayers('gbr_geo.gpkg')
gbr_geo = gpd.read_file('gbr_geo.gpkg',layer='Great Barrier Reef and Torres Strait')

#Read in AIMS data using aims.py
aims = aims
lonfilt = lambda x: aims[x].loc[aims[x]['LONGITUDE'] > 140.0]
latfilt = lambda x: aims_east[x].loc[aims_east[x]['LATITUDE'] > -25.0]

aims_east= [lonfilt(i) for i in np.arange(0,12,1)]
aims_east= [latfilt(i) for i in np.arange(0,12,1)]

aims_east = pd.concat(aims_east)

gbr_gpd = make_point(aims_east)
gbr_gpd.crs = {'init':'EPSG:4326'}

gbr_join = gpd.sjoin(gbr_gpd,gbr_geo,how='inner',op='intersects')
gbr_join['RCLASS'] =  gbr_join['class']
gbr_join = gbr_join.drop(['class'],axis=1)
gbr_join.to_csv('gbr_join.csv',index=False)

#Timor Sea
timor_geo = gpd.read_file('timor_geo.geojson')
lonfilt = lambda x: aims[x].loc[aims[x]['LONGITUDE'] < 143.0]
latfilt = lambda x: aims_north[x].loc[aims_north[x]['LATITUDE'] > -19.0]

aims_north= [lonfilt(i) for i in np.arange(0,12,1)]
aims_north= [latfilt(i) for i in np.arange(0,12,1)]

aims_north = pd.concat(aims_north)

tim_gpd = make_point(aims_north)
tim_gpd.crs = {'init':'EPSG:4326'}

tim_join = gpd.sjoin(tim_gpd,timor_geo,how='inner',op='intersects')
tim_join['RCLASS'] =  tim_join['class']
tim_join = tim_join.drop(['class'],axis=1)
tim_join.to_csv('tim_join.csv',index=False)

#Solomon Islands
solomon_geo = gpd.read_file('solomon_geo.geojson')

lonfilt = lambda x: aims[x].loc[aims[x]['LONGITUDE'] > 132.0]
latfilt = lambda x: aims_sol[x].loc[aims_sol[x]['LATITUDE'] > -14.0]

aims_sol= [lonfilt(i) for i in np.arange(0,12,1)]
aims_sol= [latfilt(i) for i in np.arange(0,12,1)]

aims_sol = pd.concat(aims_sol)

sol_gpd = make_point(aims_sol)
sol_gpd.crs = {'init': 'EPSG:4326'}

sol_join  = gpd.sjoin(sol_gpd,solomon_geo,how='inner',op='intersects')
sol_join['RCLASS'] = sol_join['class']
solomon_join = sol_join.drop(['class'],axis=1)
solomon_join.to_csv('solomon_join.csv',index=False)

#Coral Sea
fiona.listlayers('coralsea_geo.gpkg')
coral_geo = gpd.read_file('coralsea_geo.gpkg',layer='Coral Sea')

lonfilt = lambda x: aims[x].loc[aims[x]['LONGITUDE'] > 146.2]
latfilt = lambda x: aims_cor[x].loc[aims_cor[x]['LATITUDE'] < -13.5]
latfilt2 = lambda x: aims_cor[x].loc[aims_cor[x]['LATITUDE'] > -25.6]

aims_cor= [lonfilt(i) for i in np.arange(0,12,1)]
aims_cor= [latfilt(i) for i in np.arange(0,12,1)]
aims_cor= [latfilt2(i) for i in np.arange(0,12,1)]

aims_cor = pd.concat(aims_cor)

cor_gpd = make_point(aims_cor)
cor_gpd.crs = {'init':'EPSG:4326'}

cor_join = gpd.sjoin(cor_gpd,coral_geo,how='inner',op='intersects')
cor_join['RCLASS'] = cor_join['class']
coralsea_join = cor_join.drop(['class'],axis=1)
coralsea_join.to_csv('coralsea_join.csv',index=False)

#Western Australia
fiona.listlayers('westaustralia_geo.gpkg')
westaustralia_geo = gpd.read_file('westaustralia_geo.gpkg',layer='Western Australia')

lonfilt = lambda x: aims[x].loc[aims[x]['LONGITUDE'] < 123.0]
latfilt = lambda x: aims_west[x].loc[aims_west[x]['LATITUDE'] < -16.0]

aims_west= [lonfilt(i) for i in np.arange(0,12,1)]
aims_west= [latfilt(i) for i in np.arange(0,12,1)]

aims_west = pd.concat(aims_west)

west_gpd = make_point(aims_west)
west_gpd.crs = {'init':'EPSG:4326'}

west_join = gpd.sjoin(west_gpd,westaustralia_geo,how='inner',op='intersects')
west_join['RCLASS'] = west_join['class']
westaus_join = west_join.drop(['class'],axis=1)
westaus_join.to_csv('westaustralia_join.csv',index=False)

geo_merges = [hawaii_join,marianas_join,flkeys_join,timorleste_join,gbr_join,tim_join,solomon_join,coralsea_join,westaus_join]
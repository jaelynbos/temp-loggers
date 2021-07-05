#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Thu Mar 25 19:19:15 2021

@author: Jaelyn Bos
"""
'''
Import required packages
'''
import os
import glob
import numpy as np
import fiona
import pandas as pd
import geopandas as gpd
from shapely.geometry import Point

'''
Read in AIMS and NOAA data (previously processed)
'''
aimspath = '/home/jtb188/Documents/aims/'
aims = [pd.read_pickle(f) for f in sorted(glob.glob(aimspath + "*pkl"))]

noaapath = '/home/jtb188/Documents/NOAA_data/'
noaa = [pd.read_pickle(f) for f in sorted(glob.glob(noaapath+ "*.pkl"))]

'''
Change directory to location of Allen Coral Atlas geomorphic data
'''
os.chdir('/home/jtb188/Documents/allen_coral_atlas/')

'''
Function to create point from latitude and longitude
'''
def make_point(df):
    df['POINTS'] = df['LONGITUDE'].astype(str) + "_" + df['LATITUDE'].astype(str)
    df['COORDS'] = df[['LONGITUDE','LATITUDE']].values.tolist()
    df['COORDS'] = df['COORDS'].apply(Point)
    df_gpd = gpd.GeoDataFrame(df,geometry='COORDS')
    return(df_gpd)

'''
Function to recombine point/geomorphic data with complete temperature data
'''
def remerge(df_joined,datlist):
    df_joined['POINTS'] = df_joined['LONGITUDE'].astype(str) + "_" + df_joined['LATITUDE'].astype(str)
    x = [None]*len(datlist)
    for i in np.arange(0,len(datlist),1):
        x[i] = pd.merge(datlist[i],df_joined)
    return(x)

'''
Aggregate temperature data by latitute/longitude points
'''
di = {'LATITUDE':['first'],'LONGITUDE':'first'}
aims_agg = [None]*len(aims)
noaa_agg = [None]*len(noaa)

for i in np.arange(0,len(aims),1):
    aims_agg[i] = aims[i].groupby('POINTS').agg(di).reset_index()

for i in np.arange(0,len(noaa),1):
    noaa_agg[i] = noaa[i].groupby('POINTS').agg(di).reset_index()
    
'''
Concat lists to pandas dataframes
'''

aims_agg = pd.concat(aims_agg)
noaa_agg = pd.concat(noaa_agg)

'''
Make points using above function
'''
aims_agg.columns = ['POINTS','LATITUDE','LONGITUDE']
noaa_agg.columns = ['POINTS','LATITUDE','LONGITUDE']

aims_agg = make_point(aims_agg)
noaa_agg = make_point(noaa_agg)

'''
Correct coordiante reference system
'''
aims_agg.crs = {'init':'EPSG:4326'}
noaa_agg.crs = {'init':'EPSG:4326'}

'''
Import Allen Coral Atlas geomorphic data
'''
fiona.listlayers('coralsea_geo.gpkg')
coral_geo = gpd.read_file('coralsea_geo.gpkg',layer='Coral Sea')

flkeys_geo = gpd.read_file('flkeys_geo.geojson')

fiona.listlayers('gbr_geo.gpkg')
gbr_geo = gpd.read_file('gbr_geo.gpkg',layer='Great Barrier Reef and Torres Strait')

hawaii_geo = gpd.read_file('hawaii_geo.geojson')

secaribbean_geo = gpd.read_file('secarib_geo.gpkg',layer='Southeastern Caribbean')

solomon_geo = gpd.read_file('solomon_geo.geojson')

fiona.listlayers('swpacific_geo.gpkg')
swpacific_geo = gpd.read_file('swpacific_geo.gpkg',layer='Southwestern Pacific')

timor_geo = gpd.read_file('timor_geo.geojson')

fiona.listlayers('westaustralia_geo.gpkg')
westaustralia_geo = gpd.read_file('westaustralia_geo.gpkg',layer='Western Australia')

fiona.listlayers('westmicro_geo.gpkg')
westmicro_geo = gpd.read_file('westmicro_geo.gpkg',layer = 'Western Micronesia')

'''
Split up Great Barrier Reef data to be smaller
'''
gbr_geo1 = gbr_geo.iloc[0:158674,:]
gbr_geo2 = gbr_geo.iloc[158675:317349,:]
gbr_geo3 = gbr_geo.iloc[317349:476022,:]

'''
Extract geomorphic data to logger points using spatial joins
'''
coralsea = remerge(pd.DataFrame(gpd.sjoin(aims_agg,coral_geo,how='inner',op='within')),aims)
flkeys = remerge(pd.DataFrame(gpd.sjoin(noaa_agg,flkeys_geo,how='inner',op='within')),noaa)
gbr1 = remerge(pd.DataFrame(gpd.sjoin(aims_agg,gbr_geo1,how='inner',op='within')),aims)
gbr2 = remerge(pd.DataFrame(gpd.sjoin(aims_agg,gbr_geo2,how='inner',op='within')),aims)
gbr3 = remerge(pd.DataFrame(gpd.sjoin(aims_agg,gbr_geo3,how='inner',op='within')),aims)
hawaii = remerge(pd.DataFrame(gpd.sjoin(noaa_agg,hawaii_geo,how='inner',op='within')),noaa)
secarib = remerge(pd.DataFrame(gpd.sjoin(noaa_agg,secaribbean_geo,how='inner',op='within')),noaa)
solomon = remerge(pd.DataFrame(gpd.sjoin(aims_agg,solomon_geo,how='inner',op='within')),aims)
swpacific = remerge(pd.DataFrame(gpd.sjoin(noaa_agg,swpacific_geo,how='inner',op='within')),noaa)
timorsea = remerge(pd.DataFrame(gpd.sjoin(aims_agg,timor_geo,how='inner',op='within')),aims)
westaus = remerge(pd.DataFrame(gpd.sjoin(aims_agg,westaustralia_geo,how='inner',op='within')),aims)
westmicro = remerge(pd.DataFrame(gpd.sjoin(noaa_agg,westmicro_geo,how='inner',op='within')),noaa)
del gbr_geo, timor_geo,solomon_geo,coral_geo,westaustralia_geo,hawaii_geo,flkeys_geo,secaribbean_geo,swpacific_geo,westmicro_geo

'''
Concat lists to dataframes
'''
os.chdir('/home/jtb188/Documents/allen_coral_atlas/agg_merge/')

coralsea = pd.concat(coralsea)
flkeys = pd.concat(flkeys)
gbr1 = pd.concat(gbr1)
gbr2 = pd.concat(gbr2)
gbr3 = pd.concat(gbr3)
hawaii = pd.concat(hawaii)
secarib = pd.concat(secarib)
solomon = pd.concat(solomon)
swpacific = pd.concat(swpacific)
timorsea = pd.concat(timorsea)
westaus = pd.concat(westaus)
westmicro = pd.concat(westmicro)

'''
Export merged data as pickles
'''
coralsea.to_pickle('coralsea.pkl')
flkeys.to_pickle('flkeys.pkl')
gbr1.to_pickle('gbr1.pkl')
gbr2.to_pickle('gbr2.pkl')
gbr3.to_pickle('gbr3.pkl')
hawaii.to_pickle('hawaii.pkl')
secarib.to_pickle('secarib.pkl')
solomon.to_pickle('solomon.pkl')
swpacific.to_pickle('swpacific.pkl')
timorsea.to_pickle('timorsea.pkl')
westaus.to_pickle('westaus.pkl')
westmicro.to_pickle('westmicro.pkl')
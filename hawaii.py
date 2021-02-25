# -*- coding: utf-8 -*-
"""
Spyder Editor

This is a temporary script file.
"""
import os
import fiona
import pandas as pd
import geopandas as gpd
from shapely.geometry import Point

os.chdir('/home/jtb188/Documents/allen_coral_atlas/')

hawaii_geo = gpd.read_file('hawaii_geo.geojson')

path0 = "/home/jtb188/Documents/NOAA_data/"
os.chdir(path0)
keepcols = ['LATITUDE','LONGITUDE','DEPTH','TIMESTAMP','TEMP_C']

#Read in Hawaii data
hawaii_1 = pd.read_csv("hawaii_temps/hawaii_temps_1/2.2/data/0-data/unzipped/ESD_NCRMP_Temperature_2013_Hawaii.csv")
hawaii_2 = pd.read_csv("hawaii_temps/hawaii_temps_2/ESD_NCRMP_Temperature_2016_Hawaii/ESD_NCRMP_Temperature_2016_Hawaii.csv")
hawaii_3 = pd.read_csv("hawaii_temps/hawaii_temps_3/ESD_NCRMP_Temperature_2019_Hawaii/ESD_NCRMP_Temperature_2019_Hawaii.csv")
hawaii = pd.concat([hawaii_1,hawaii_2,hawaii_3],axis=0,ignore_index=True,join="outer")
hawaii['TIMESTAMP'] = pd.to_datetime(hawaii['TIMESTAMP'], format='%Y-%m-%d %H:%M:%S')
noaa_hawaii = hawaii[keepcols]
del hawaii, hawaii_1,hawaii_2,hawaii_3

noaa_hawaii['POINTS'] = noaa_hawaii['LONGITUDE'].astype(str) + "_" + noaa_hawaii['LATITUDE'].astype(str)
noaa_hawaii['COORDS'] = noaa_hawaii[['LONGITUDE','LATITUDE']].values.tolist()
noaa_hawaii['COORDS'] = noaa_hawaii['COORDS'].apply(Point)

hawaii_gpd = gpd.GeoDataFrame(noaa_hawaii,geometry='COORDS')

hawaii_geo.crs
hawaii_gpd.crs = {'init':'EPSG:4326'}

hawaii_join = gpd.sjoin(hawaii_gpd,hawaii_geo,how='inner',op='intersects')
hawaii_join.to_csv('hawaii_join.csv',index=False)

hawaii_join.columns = ['LATITUDE', 'LONGITUDE', 'DEPTH', 'TIMESTAMP', 'TEMP_C', 'POINTS', 'COORDS', 'index_right', 'fill-opacity', 'stroke', 'stroke-opacity','RCLASS', 'geometry']
path0 = "/home/jtb188/Documents/NOAA_data/"

hawaii_join = gpd.read_file('hawaii_join.csv')

hawaii_join['TIMESTAMP'] = pd.to_datetime(hawaii_join['TIMESTAMP'])

hawaii_join['DEPTH'] = pd.to_numeric(hawaii_join['DEPTH'])
hawaii_join['TEMP_C'] = pd.to_numeric(hawaii_join['TEMP_C'])

hawaii_join['YEAR'] = hawaii_join['TIMESTAMP'].dt.year
hawaii_join['DAYS'] = hawaii_join['TIMESTAMP'].dt.date

#Daily min and max
hawaii_join['dmin'] = hawaii_join.groupby(['POINTS','DAYS']).TEMP_C.transform('min')
hawaii_join['dmax'] = hawaii_join.groupby(['POINTS','DAYS']).TEMP_C.transform('max')
hawaii_join['DAILY_DIFF'] = hawaii_join['dmax'] - hawaii_join['dmin']
hawaii_join = hawaii_join.drop(columns=['dmin','dmax'])

hawaii2014 = hawaii_join[hawaii_join['YEAR']==2014]
hawaii2014.to_csv('hawaii2014.csv',index=False)
hawaii2015 = hawaii_join[hawaii_join['YEAR']==2015]
hawaii2015.to_csv('hawaii2015.csv',index=False)

agg_dic = {'TEMP_C': ['median'],'LONGITUDE':['first'],'LATITUDE':['first'],'DEPTH':['median'],'RCLASS':['first']}

#Medians
hawaii_meds14 = hawaii2014.groupby('POINTS')['TEMP_C','LONGITUDE','LATITUDE','DEPTH','RCLASS'].agg(agg_dic)
hawaii_meds14.to_csv('hawaii_medians2014.csv',index=False)

hawaii_meds15 = hawaii2015.groupby('POINTS')['TEMP_C','LONGITUDE','LATITUDE','DEPTH','RCLASS'].agg(agg_dic)
hawaii_meds15.to_csv('hawaii_medians2015.csv',index=False)

#99th percentile
q99 = lambda x: x.quantile(q=.99)

agg_dic = {'TEMP_C':q99, 'LONGITUDE':['first'],'LATITUDE':['first'],'DEPTH':['median'],'RCLASS':['first']}
hawaii_q9914 = hawaii2014.groupby('POINTS')['TEMP_C','LONGITUDE','LATITUDE','DEPTH','RCLASS'].agg(agg_dic)
hawaii_q9915 = hawaii2015.groupby('POINTS')['TEMP_C','LONGITUDE','LATITUDE','DEPTH','RCLASS'].agg(agg_dic)


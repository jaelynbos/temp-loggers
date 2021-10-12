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
import fiona
from shapely.geometry import Point

dongsha_xr = xr.open_dataset('/home/jtb188/Documents/Dongsha/Dongsha_temps.netcdf')

dongsha = pd.DataFrame()
dongsha['LATITUDE'] = dongsha_xr['lat']
dongsha['LONGITUDE'] = dongsha_xr['lon']
dongsha['DEPTH'] = dongsha_xr['depth']
dongsha['ISO_DateTime_UTC'] = dongsha_xr['ISO_DateTime_UTC']
dongsha['time'] = dongsha_xr['time']
dongsha['month'] = dongsha_xr['mon']
dongsha['year'] = dongsha_xr['year']
dongsha['day'] = dongsha_xr['day']
dongsha['TEMP_C'] = dongsha_xr['temp']

'''
Convert variables to numeric in the least sensible and most cumbersome way possible
Wtf is 'bytes4104' data type?
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

'''
Construct a datetime variable from pieces because nothing is easy in this life
'''

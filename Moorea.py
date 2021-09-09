#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Fri Aug 13 10:33:56 2021

@author: jtb188
"""

import numpy as np
import pandas as pd
import geopandas as gpd
import fiona
from shapely.geometry import Point

moorea0 = pd.read_csv('/home/jtb188/Documents/Moorea/MCR_LTER00_BottomMountThermistors_20200302.csv')
moorea1 = pd.read_csv('/home/jtb188/Documents/Moorea/MCR_LTER01_BottomMountThermistors_20200306.csv')
moorea2 = pd.read_csv('/home/jtb188/Documents/Moorea/MCR_LTER02_BottomMountThermistors_20200306.csv')
moorea3 = pd.read_csv('/home/jtb188/Documents/Moorea/MCR_LTER03_BottomMountThermistors_20200306.csv')
moorea4 = pd.read_csv('/home/jtb188/Documents/Moorea/MCR_LTER04_BottomMountThermistors_20200306.csv')
moorea5 = pd.read_csv('/home/jtb188/Documents/Moorea/MCR_LTER05_BottomMountThermistors_20200306.csv')
moorea6 = pd.read_csv('/home/jtb188/Documents/Moorea/MCR_LTER06_BottomMountThermistors_20200306.csv')

moorea7 = pd.read_csv('/home/jtb188/Documents/Moorea/CTD_GUMPR_20120227.csv')

print(*moorea1.columns)
print(*moorea2.columns)

moorea7['LATITUDE'] = -17.490
moorea7['LONGITUDE'] = -149.826

'''
Add geomorphic data
'''
moorea_geo = gpd.read_file('/home/jtb188/Documents/Moorea/moorea_geo.geojson')
#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Wed Feb 10 09:05:37 2021

@author: jtb188
"""
import pandas as pd
import os
import glob
import numpy as np
from datetime import datetime

path0 = "/home/jtb188/Documents/aims/aims_csvs/"
os.chdir(path0)

aims_data = ([pd.read_csv(f) for f in glob.glob("*.csv")])

#The only one with reasonable column names
aims_data[5].columns

aims_data1 = aims_data

for i in np.arange(0,12,1):
    aims_data1[i].columns = aims_data1[5].columns
    
timefilt = lambda x: aims_data1[x].loc[pd.to_datetime(aims_data1[x]['time'],errors='coerce') > datetime(2010,1,1)]
aims_filt= [timefilt(i) for i in np.arange(0,12,1)]

#qcfilt = lambda x: aims_filt[i].loc[aims_filt[i]['qc_flag'] < 3.0]
    
#aims_filt= [qcfilt(i) for i in np.arange(0,12,1)]

keepcols = ['latitude','longitude','depth','time','level1_value','qc_flag']

for i in np.arange(0,12,1):
    aims_filt[i] = aims_filt[i][keepcols]
    
for i in np.arange(0,12,1):
    aims_filt[i]['TIMESTAMP'] =pd.to_datetime(aims_filt[i]['time'])
    aims_filt[i]['DEPTH'] = aims_filt[i]['depth']
    aims_filt[i]['LATITUDE'] = aims_filt[i]['latitude']
    aims_filt[i]['LONGITUDE'] = aims_filt[i]['longitude']
    aims_filt[i]['TEMP_C'] = aims_filt[i]['level1_value']

for i in np.arange(0,len(aims_filt),1):
    aims_filt[i]['POINTS'] = aims_filt[i]['LONGITUDE'].astype(str) + "_" + aims_filt[i]['LATITUDE'].astype(str)
    
keepcols = ['LATITUDE','LONGITUDE','DEPTH','TIMESTAMP','TEMP_C','qc_flag','POINTS']

for i in np.arange(0,12,1):
    aims_filt[i] = aims_filt[i][keepcols]
    
aims = aims_filt
del aims_filt

#Export to pickle
for i in np.arange(0,len(aims),1):
    aims[i].to_pickle('aims'+str(i)+'.pkl')
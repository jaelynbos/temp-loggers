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

path0 = "/home/jtb188/Documents/aims/"
os.chdir(path0)

aims_data = ([pd.read_csv(f) for f in glob.glob("*.csv")])

#The only one with reasonable column names
aims_data[5].columns

aims_data1 = aims_data

for i in np.arange(0,12,1):
    aims_data1[i].columns = aims_data1[5].columns
    
timefilt = lambda x: aims_data1[x].loc[pd.to_datetime(aims_data1[x]['time'],errors='coerce') > datetime(2005,1,1)]

aims_filt= [timefilt(i) for i in np.arange(0,12,1)]

qcfilt = lambda x: aims_filt[i].loc[aims_filt[i]['qc_flag'] < 3.0]
    
aims_filt= [qcfilt(i) for i in np.arange(0,12,1)]

for i in np.arange(0,12,1):
    aims_data[i]['TIMESTAMP'] =pd.to_datetime(aims_data[i]['time'])
    aims_data[i]['DEPTH'] = aims_data[i]['depth']
    aims_data[i]['LATITUDE'] = aims_data[i]['latitude']
    aims_data[i]['LONGITUDE'] = aims_data[i]['longitude']
    aims_data[i]['TEMP_C'] = aims_data[i]['level1_value']

keepcols = ['LATITUDE','LONGITUDE','DEPTH','TIMESTAMP','TEMP_C']

for i in np.arange(0,12,1):
    aims_data[i] = aims_data[i][keepcols]
    

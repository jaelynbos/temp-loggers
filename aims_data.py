#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Wed Feb 10 09:05:37 2021

@author: jtb188
"""
import pandas as pd
import os
import glob

path0 = "/home/jtb188/Documents/aims/"
os.chdir(path0)



aims_data = ([pd.read_csv(f) for f in glob.glob("*.csv")])

#The only one with reasonable column names
aims_data[5].columns
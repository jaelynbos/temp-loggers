#!/usr/bin/env python
# coding: utf-8

import pandas as pd
import os
import numpy as np
import glob
import pickle 

#Set baseline path
path0 = "/home/jtb188/Documents/NOAA_data/"
os.chdir(path0)

keepcols = ['LATITUDE','LONGITUDE','DEPTH','TIMESTAMP','TEMP_C']

#Read in American Samoa data
american_samoa = pd.read_csv("american_samoa_temps/2.2/data/0-data/unzipped/ESD_NCRMP_Temperature_2015_AMSM.csv")
american_samoa['TIMESTAMP'] = pd.to_datetime(american_samoa['TIMESTAMP'], format='%Y-%m-%d %H:%M:%S')
noaa_amsam = american_samoa[keepcols]
del american_samoa

#Read in Batangas data
batangas = pd.read_csv("batangas_philippines_temps/1.1/data/0-data/MV_OCN_STR_TIMESERIES_Philippines.csv")
batangas = batangas.rename(columns={'TEMPERATURE':'TEMP_C'})
dummy = batangas['YEAR'].astype(str)+ batangas['MONTH'].astype(str).str.zfill(2) + batangas['DAY'].astype(str).str.zfill(2) +batangas['HOUR'].astype(str).str.zfill(2) +batangas['MINUTE'].astype(str).str.zfill(2)+batangas['SECOND'].astype(str).str.zfill(2) 
batangas['TIMESTAMP'] = pd.to_datetime(dummy,format='%Y%m%d%H%M%S')
noaa_batangas = batangas[keepcols]
del dummy, batangas

#Read in Biscayne data
path = path0 + "biscayne_florida_temps/1.1/data/0-data/NCRMP_WaterTemperatureData_BiscayneNationalPark_2014-2017/NCRMP_STR_BNP/"
biscayne = pd.concat([pd.read_csv(f) for f in glob.glob(path+ "*.csv")], axis=0, ignore_index=True,join="outer")
biscayne['TIMESTAMP'] = pd.to_datetime(biscayne['TIMESTAMP'],format='%m/%d/%Y %H:%M:%S')
noaa_biscayne = biscayne[keepcols]
del biscayne

#Read in Hawaii data
hawaii_1 = pd.read_csv("hawaii_temps/hawaii_temps_1/2.2/data/0-data/unzipped/ESD_NCRMP_Temperature_2013_Hawaii.csv")
hawaii_2 = pd.read_csv("hawaii_temps/hawaii_temps_2/ESD_NCRMP_Temperature_2016_Hawaii/ESD_NCRMP_Temperature_2016_Hawaii.csv")
hawaii_3 = pd.read_csv("hawaii_temps/hawaii_temps_3/ESD_NCRMP_Temperature_2019_Hawaii/ESD_NCRMP_Temperature_2019_Hawaii.csv")
hawaii = pd.concat([hawaii_1,hawaii_2,hawaii_3],axis=0,ignore_index=True,join="outer")
hawaii['TIMESTAMP'] = pd.to_datetime(hawaii['TIMESTAMP'], format='%Y-%m-%d %H:%M:%S')
noaa_hawaii = hawaii[keepcols]
del hawaii, hawaii_1,hawaii_2,hawaii_3

#Read in Marianas Islands data
marianas = pd.read_csv("marianas_temps/ESD_NCRMP_Temperature_2017_Marianas.csv")
marianas['TIMESTAMP'] = pd.to_datetime(marianas['TIMESTAMP'], format='%Y-%m-%d %H:%M:%S')
noaa_marianas = marianas[keepcols]
del marianas

#Read in Pacific Remote Island Areas data
path = path0 + "pria_temps/pria_temps_1/"
pria_1 = ([pd.read_csv(f) for f in glob.glob(path+ "*.csv")])
pria_1 = pd.concat(pria_1,axis=0,ignore_index=True,join="outer")

path = path0 + "pria_temps/pria_temps_2/2.2/data/data/unzipped/split/"
pria_2 = ([pd.read_csv(f) for f in glob.glob(path+ "*.csv")])
pria_2 = pd.concat(pria_2,axis=0,ignore_index=True,join="outer")
pria = pd.concat([pria_1,pria_2], axis=0, ignore_index=True,join="outer")
del pria_1,pria_2
pria['TIMESTAMP'] = pd.to_datetime(pria['TIMESTAMP'])
noaa_pria = pria[keepcols]
del pria

#Fix messy St Croix data
path = path0 + "stcroix_temps/stcroix_temps_2/2.2/data/0-data/NCRMP_STR_Data_STX_2019/"
stcroix_1 = ([pd.read_csv(f) for f in glob.glob(path+ "*.csv")])
stc_cols = ['SITETAG', 'LATITUDE', 'LONGITUDE', 'DEPTH', 'REGIONCODE',
       'LOCATIONCODE', 'INSTRUMENT', 'INSTRUMENTSN', 'DEPLOYCRUISE',
       'DEPLOYUTC', 'RETRIEVECRUISE', 'RETRIEVEUTC', 'VALIDDATASTART',
       'VALIDDATAEND', 'YEAR', 'MONTH', 'DAY', 'HOUR','TIMESTAMP','TEMP_C']
stcroix_1[6].columns  = stc_cols
stcroix_1[11].columns = stc_cols
stcroix_1[12].columns = stc_cols

stcroix_1[8].columns = ['SITETAG', 'LATITUDE', 'LONGITUDE', 'DEPTH', 'REGIONCODE',
       'LOCATIONCODE', 'INSTRUMENT', 'INSTRUMENTSN', 'DEPLOYCRUISE',
       'DEPLOYUTC', 'RETRIEVECRUISE', 'RETRIEVEUTC', 'VALIDDATASTART',
       'VALIDDATAEND', 'YEAR', 'MONTH', 'DAY', 'TIMESTAMP', 'HOUR', 'TEMP_C']

stcroix_1 = pd.concat(stcroix_1,axis=0,ignore_index=True,join="outer")

#Read in good St Croix data
path = path0 + "stcroix_temps/stcroix_temps_1/1.1/data/0-data/NCRMP_STR_StCroix_WaterTemperatureData_2013-9-9_to_2016-9-14/NCRMP_STR_STCROIX_EAST/"
stcroix_2 = pd.concat([pd.read_csv(f) for f in glob.glob(path+ "*.csv")], axis=0, ignore_index=True,join="outer")

path = path0 + "stcroix_temps/stcroix_temps_1/1.1/data/0-data/NCRMP_STR_StCroix_WaterTemperatureData_2013-9-9_to_2016-9-14/NCRMP_STR_STCROIX_NORTH/"
stcroix_3 = pd.concat([pd.read_csv(f) for f in glob.glob(path+ "*.csv")], axis=0, ignore_index=True,join="outer")

path = path0 + "stcroix_temps/stcroix_temps_1/1.1/data/0-data/NCRMP_STR_StCroix_WaterTemperatureData_2013-9-9_to_2016-9-14/NCRMP_STR_STCROIX_SOUTH/"
stcroix_4 = pd.concat([pd.read_csv(f) for f in glob.glob(path+ "*.csv")], axis=0, ignore_index=True,join="outer")

path = path0 + "stcroix_temps/stcroix_temps_1/1.1/data/0-data/NCRMP_STR_StCroix_WaterTemperatureData_2013-9-9_to_2016-9-14/NCRMP_STR_STCROIX_WEST/"
stcroix_5 = pd.concat([pd.read_csv(f) for f in glob.glob(path+ "*.csv")], axis=0, ignore_index=True,join="outer")

stcroix = pd.concat([stcroix_1,stcroix_2,stcroix_3,stcroix_4,stcroix_5],axis=0,ignore_index=True,join="outer")
dummy = stcroix['TIMESTAMP'] + "_" + stcroix['HOUR']
stcroix['TIMESTAMP'] = pd.to_datetime(dummy,format='%m/%d/%y_%H:%M:%S',errors='coerce')

noaa_stcroix = stcroix[keepcols]
del stcroix_1,stcroix_2,stcroix_3,stcroix_4,stcroix_5,dummy,stcroix

#Read in St John data
path = path0 + "stthomas_stjohn_temps/1.1/data/0-data/NCRMP_WaterTemperatureData_STJOHN_STHOMAS_2014-2017/NCRMP_STR_STJOHN_EAST/"
stjohn_1 = pd.concat([pd.read_csv(f) for f in glob.glob(path+ "*.csv")], axis=0, ignore_index=True,join="outer")

path = path0 + "stthomas_stjohn_temps/1.1/data/0-data/NCRMP_WaterTemperatureData_STJOHN_STHOMAS_2014-2017/NCRMP_STR_STJOHN_NORTH/"
stjohn_2 = pd.concat([pd.read_csv(f) for f in glob.glob(path+ "*.csv")], axis=0, ignore_index=True,join="outer")

path = path0 + "stthomas_stjohn_temps/1.1/data/0-data/NCRMP_WaterTemperatureData_STJOHN_STHOMAS_2014-2017/NCRMP_STR_STJOHN_SOUTH/"
stjohn_3 = pd.concat([pd.read_csv(f) for f in glob.glob(path+ "*.csv")], axis=0, ignore_index=True,join="outer")

path = path0 + "stthomas_stjohn_temps/1.1/data/0-data/NCRMP_WaterTemperatureData_STJOHN_STHOMAS_2014-2017/NCRMP_STR_STJOHN_WEST/"
stjohn_4 = pd.concat([pd.read_csv(f) for f in glob.glob(path+ "*.csv")], axis=0, ignore_index=True,join="outer")

stjohn = pd.concat([stjohn_1,stjohn_2,stjohn_3,stjohn_4],axis=0,ignore_index=True,join="outer")
stjohn['TIMESTAMP'] = pd.to_datetime(stjohn['TIMESTAMP'], format='%m/%d/%Y %H:%M:%S')
noaa_stjohn = stjohn[keepcols]
del stjohn_1,stjohn_2,stjohn_3,stjohn_4, path, stjohn

#Read in St. Thomas data
path = path0 + "stthomas_stjohn_temps/1.1/data/0-data/NCRMP_WaterTemperatureData_STJOHN_STHOMAS_2014-2017/NCRMP_STR_STTHOMAS_NORTH/"
stthomas_1 = pd.concat([pd.read_csv(f) for f in glob.glob(path+ "*.csv")], axis=0, ignore_index=True,join="outer")

path = path0 + "stthomas_stjohn_temps/1.1/data/0-data/NCRMP_WaterTemperatureData_STJOHN_STHOMAS_2014-2017/NCRMP_STR_STTHOMAS_SOUTH/"
stthomas_2 = pd.concat([pd.read_csv(f) for f in glob.glob(path+ "*.csv")], axis=0, ignore_index=True,join="outer")

path = path0 + "stthomas_stjohn_temps/1.1/data/0-data/NCRMP_WaterTemperatureData_STJOHN_STHOMAS_2014-2017/NCRMP_STR_STTHOMAS_WEST/"
stthomas_3 = pd.concat([pd.read_csv(f) for f in glob.glob(path+ "*.csv")], axis=0, ignore_index=True,join="outer")

stthomas = pd.concat([stthomas_1,stthomas_2,stthomas_3],axis=0,ignore_index=True,join="outer")
stthomas['TIMESTAMP'] = pd.to_datetime(stthomas['TIMESTAMP'], format='%m/%d/%Y %H:%M:%S')
noaa_stthomas = stthomas[keepcols]
del stthomas_1,stthomas_2,stthomas_3, path, stthomas

#Read in Timor-Leste data
timor_leste = pd.read_csv("timor_leste_temps/1.1/data/0-data/STR_Temperature_Timor_2014.csv")
timor_leste['DEPTH'] = timor_leste['DEPTH_M']
dummy = timor_leste['_DATE'].astype(str)+ "_" + timor_leste['HOUR'].astype(str).str.zfill(2)
timor_leste['TIMESTAMP'] = pd.to_datetime(dummy,format='%m/%d/%Y_%H')
noaa_timorleste = timor_leste[keepcols]
del dummy, timor_leste

#Read in Wake Island data
wake = pd.read_csv("wake_island_temps/2.2/data/0-data/unzipped/ESD_NCRMP_Temperature_2014_PRIA.csv")
wake['TIMESTAMP'] = pd.to_datetime(wake['TIMESTAMP'])
noaa_wake = wake[keepcols]
del wake 

#Read in Puerto Rico data
path = path0 + "puerto_rico_temps/1.1/data/0-data/NCRMP_WaterTemperatureData_PuertoRico_2015-2017/NCRMP_STR_PR_EAST/"
pr_1 = pd.concat([pd.read_csv(f) for f in glob.glob(path+ "*.csv")], axis=0, ignore_index=True,join="outer")

path = path0 + "puerto_rico_temps/1.1/data/0-data/NCRMP_WaterTemperatureData_PuertoRico_2015-2017/NCRMP_STR_PR_NORTH/"
pr_2 = pd.concat([pd.read_csv(f) for f in glob.glob(path+ "*.csv")], axis=0, ignore_index=True,join="outer")

path = path0 + "puerto_rico_temps/1.1/data/0-data/NCRMP_WaterTemperatureData_PuertoRico_2015-2017/NCRMP_STR_PR_SOUTH/"
pr_3 = pd.concat([pd.read_csv(f) for f in glob.glob(path+ "*.csv")], axis=0, ignore_index=True,join="outer")

path = path0 + "puerto_rico_temps/1.1/data/0-data/NCRMP_WaterTemperatureData_PuertoRico_2015-2017/NCRMP_STR_PR_WEST/"
pr_4 = pd.concat([pd.read_csv(f) for f in glob.glob(path+ "*.csv")], axis=0, ignore_index=True,join="outer")

puerto_rico = pd.concat([pr_1,pr_2,pr_3,pr_4],axis=0,ignore_index=True,join="outer")
puerto_rico['TIMESTAMP'] = pd.to_datetime(puerto_rico['TIMESTAMP'],errors='coerce')
noaa_pr = puerto_rico[keepcols]
del pr_1,pr_2,pr_3,pr_4,path,puerto_rico

#Read in other Florida Keys data
path = path0 + "florida_keys_temps/1.1/data/0-data/NCRMP_FloridaKeys_STR_Data_2013-12-03_to_2016-12-13/NCRMP_STR_FL_KEYS_LOW/"
keys_low =  pd.concat([pd.read_csv(f) for f in glob.glob(path+ "*.csv")], axis=0, ignore_index=True,join="outer")

path = path0 + "florida_keys_temps/1.1/data/0-data/NCRMP_FloridaKeys_STR_Data_2013-12-03_to_2016-12-13/NCRMP_STR_FL_KEYS_MID/"
keys_mid =  pd.concat([pd.read_csv(f) for f in glob.glob(path+ "*.csv")], axis=0, ignore_index=True,join="outer")

path = path0 + "florida_keys_temps/1.1/data/0-data/NCRMP_FloridaKeys_STR_Data_2013-12-03_to_2016-12-13/NCRMP_STR_FL_KEYS_UPP/"
keys_upp =  pd.concat([pd.read_csv(f) for f in glob.glob(path+ "*.csv")], axis=0, ignore_index=True,join="outer")

florida_keys = pd.concat([keys_low,keys_mid,keys_upp],axis=0,ignore_index=True,join="outer")
dummy = florida_keys['TIMESTAMP'] + "_" + florida_keys['HOUR']
florida_keys['TIMESTAMP'] = pd.to_datetime(dummy,format='%m/%d/%y_%H:%M:%S',errors='coerce')
noaa_keys = florida_keys[keepcols]
del keys_low,keys_mid,keys_upp,path,dummy,florida_keys

noaa = [noaa_amsam,noaa_batangas,noaa_biscayne,noaa_hawaii,noaa_keys,noaa_marianas,noaa_pria,noaa_pr,noaa_stcroix,noaa_stjohn,noaa_stthomas,noaa_timorleste,noaa_wake]

for i in np.arange(0,len(noaa),1):
    noaa[i]['POINTS'] = noaa[i]['LONGITUDE'].astype(str) + "_" + noaa[i]['LATITUDE'].astype(str)
    
noaa[0].to_pickle('amsam.pkl')
noaa[1].to_pickle('batangas.pkl')
noaa[2].to_pickle('biscayne.pkl')
noaa[3].to_pickle('hawaii.pkl')
noaa[4].to_pickle('flkeys.pkl')
noaa[5].to_pickle('marianas.pkl')
noaa[6].to_pickle('pria.pkl')
noaa[7].to_pickle('puertorico.pkl')
noaa[8].to_pickle('stcroix.pkl')
noaa[9].to_pickle('stjohn.pkl')
noaa[10].to_pickle('stthomas.pkl')
noaa[11].to_pickle('timorleste.pkl')
noaa[10].to_pickle('wake.pkl')
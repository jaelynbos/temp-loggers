#!/usr/bin/env python
# coding: utf-8

import pandas as pd
import os
import numpy as np
import glob
from datetime import datetime

#Set baseline path
path0 = "C:\\Users\\jaely\\Documents\\NOAA_data\\"
os.chdir(path0)

eqcols = lambda x,y: (y[x].columns==y[x+1].columns).all()

#Read in American Samoa data
american_samoa = pd.read_csv("american_samoa_temps\\2.2\\data\\0-data\\unzipped\\ESD_NCRMP_Temperature_2015_AMSM.csv")
print(american_samoa.shape)
print(american_samoa.columns)

#Read in Batangas data
batangas = pd.read_csv("batangas_philippines_temps\\1.1\\data\\0-data\\MV_OCN_STR_TIMESERIES_Philippines.csv")
print(batangas.shape)
print(batangas.columns)

#Read in Biscayne data
path = path0 + "biscayne_florida_temps\\1.1\\data\\0-data\\NCRMP_WaterTemperatureData_BiscayneNationalPark_2014-2017\\NCRMP_STR_BNP\\"
biscayne = pd.concat([pd.read_csv(f) for f in glob.glob(path+ "*.csv")], axis=0, ignore_index=True,join="outer")
print(biscayne.shape)

#Read in Hawaii data
hawaii_1 = pd.read_csv("hawaii_temps\\hawaii_temps_1\\2.2\\data\\0-data\\unzipped\\ESD_NCRMP_Temperature_2013_Hawaii.csv")
hawaii_2 = pd.read_csv("hawaii_temps\\hawaii_temps_2\\ESD_NCRMP_Temperature_2016_Hawaii\\ESD_NCRMP_Temperature_2016_Hawaii.csv")
hawaii_3 = pd.read_csv("hawaii_temps\\hawaii_temps_3\\ESD_NCRMP_Temperature_2019_Hawaii\\ESD_NCRMP_Temperature_2019_Hawaii.csv")
hawaii = pd.concat([hawaii_1,hawaii_2,hawaii_3],axis=0,ignore_index=True,join="outer")
print(hawaii.shape)
print(hawaii.columns)
del hawaii_1,hawaii_2,hawaii_3

#Read in Marianas Islands data
marianas = pd.read_csv("marianas_temps\\ESD_NCRMP_Temperature_2017_Marianas.csv")
print(marianas.shape)
print(marianas.columns)

#Read in Pacific Remote Island Areas data
pria_1 = pd.read_csv("pria_temps\\pria_temps_1\\ESD_NCRMP_Temperature_2015_PRIA\\ESD_NCRMP_Temperature_2015_PRIA.csv")
pria_2 = pd.read_csv("pria_temps\\pria_temps_2\\2.2\\data\\0-data\\ESD_NCRMP_Temperature_2017_PRIA\\ESD_NCRMP_Temperature_2017_PRIA.csv")
pria = pd.concat([pria_1,pria_2], axis=0, ignore_index=True,join="outer")
del pria_1,pria_2

#Fix messy St Croix data
path = path0 + "stcroix_temps\\stcroix_temps_2\\2.2\\data\\0-data\\NCRMP_STR_Data_STX_2019\\"
stcroix_1 = ([pd.read_csv(f) for f in glob.glob(path+ "*.csv")])

#printcol = lambda x: print(stcroix_1[x].columns)
#[printcol(i) for i in np.arange(0,len(stcroix_1),1)]
#print(len(stcroix_1))

stcroix_1[11].columns = ['SITETAG', 'LATITUDE', 'LONGITUDE', 'DEPTH', 'REGIONCODE',
       'LOCATIONCODE', 'INSTRUMENT', 'INSTRUMENTSN', 'DEPLOYCRUISE',
       'DEPLOYUTC', 'RETRIEVECRUISE', 'RETRIEVEUTC', 'VALIDDATASTART',
       'VALIDDATAEND', 'YEAR', 'MONTH', 'DAY', 'TIMESTAMP', 'HOUR', 'TEMP_C']
stcroix_1[12].columns = ['SITETAG', 'LATITUDE', 'LONGITUDE', 'DEPTH', 'REGIONCODE',
       'LOCATIONCODE', 'INSTRUMENT', 'INSTRUMENTSN', 'DEPLOYCRUISE',
       'DEPLOYUTC', 'RETRIEVECRUISE', 'RETRIEVEUTC', 'VALIDDATASTART',
       'VALIDDATAEND', 'YEAR', 'MONTH', 'DAY', 'HOUR', 'TIMESTAMP', 'TEMP_C']

stcroix_1 = pd.concat(stcroix_1,axis=0,ignore_index=True,join="outer")

#Read in good St Croix data
path = path0 + "stcroix_temps\\stcroix_temps_1\\1.1\\data\\0-data\\NCRMP_STR_StCroix_WaterTemperatureData_2013-9-9_to_2016-9-14\\NCRMP_STR_STCROIX_EAST\\"
stcroix_2 = pd.concat([pd.read_csv(f) for f in glob.glob(path+ "*.csv")], axis=0, ignore_index=True,join="outer")

path = path0 + "stcroix_temps\\stcroix_temps_1\\1.1\\data\\0-data\\NCRMP_STR_StCroix_WaterTemperatureData_2013-9-9_to_2016-9-14\\NCRMP_STR_STCROIX_NORTH\\"
stcroix_3 = pd.concat([pd.read_csv(f) for f in glob.glob(path+ "*.csv")], axis=0, ignore_index=True,join="outer")

path = path0 + "stcroix_temps\\stcroix_temps_1\\1.1\\data\\0-data\\NCRMP_STR_StCroix_WaterTemperatureData_2013-9-9_to_2016-9-14\\NCRMP_STR_STCROIX_SOUTH\\"
stcroix_4 = pd.concat([pd.read_csv(f) for f in glob.glob(path+ "*.csv")], axis=0, ignore_index=True,join="outer")

path = path0 + "stcroix_temps\\stcroix_temps_1\\1.1\\data\\0-data\\NCRMP_STR_StCroix_WaterTemperatureData_2013-9-9_to_2016-9-14\\NCRMP_STR_STCROIX_WEST\\"
stcroix_5 = pd.concat([pd.read_csv(f) for f in glob.glob(path+ "*.csv")], axis=0, ignore_index=True,join="outer")

stcroix = pd.concat([stcroix_1,stcroix_2,stcroix_3,stcroix_4,stcroix_5],axis=0,ignore_index=True,join="outer")
print(stcroix.shape)
print(stcroix.columns)

del stcroix_1,stcroix_2,stcroix_3,stcroix_4,stcroix_5


#Read in St John data
path = path0 + "stthomas_stjohn_temps\\1.1\\data\\0-data\\NCRMP_WaterTemperatureData_STJOHN_STHOMAS_2014-2017\\NCRMP_STR_STJOHN_EAST\\"
stjohn_1 = pd.concat([pd.read_csv(f) for f in glob.glob(path+ "*.csv")], axis=0, ignore_index=True,join="outer")

path = path0 + "stthomas_stjohn_temps\\1.1\\data\\0-data\\NCRMP_WaterTemperatureData_STJOHN_STHOMAS_2014-2017\\NCRMP_STR_STJOHN_NORTH\\"
stjohn_2 = pd.concat([pd.read_csv(f) for f in glob.glob(path+ "*.csv")], axis=0, ignore_index=True,join="outer")

path = path0 + "stthomas_stjohn_temps\\1.1\\data\\0-data\\NCRMP_WaterTemperatureData_STJOHN_STHOMAS_2014-2017\\NCRMP_STR_STJOHN_SOUTH\\"
stjohn_3 = pd.concat([pd.read_csv(f) for f in glob.glob(path+ "*.csv")], axis=0, ignore_index=True,join="outer")

path = path0 + "stthomas_stjohn_temps\\1.1\\data\\0-data\\NCRMP_WaterTemperatureData_STJOHN_STHOMAS_2014-2017\\NCRMP_STR_STJOHN_WEST\\"
stjohn_4 = pd.concat([pd.read_csv(f) for f in glob.glob(path+ "*.csv")], axis=0, ignore_index=True,join="outer")

stjohn = pd.concat([stjohn_1,stjohn_2,stjohn_3,stjohn_4],axis=0,ignore_index=True,join="outer")
print(stjohn.shape)
print(stjohn.columns)

del stjohn_1,stjohn_2,stjohn_3,stjohn_4, path

#Read in St. Thomas data
path = path0 + "stthomas_stjohn_temps\\1.1\\data\\0-data\\NCRMP_WaterTemperatureData_STJOHN_STHOMAS_2014-2017\\NCRMP_STR_STTHOMAS_NORTH\\"
stthomas_1 = pd.concat([pd.read_csv(f) for f in glob.glob(path+ "*.csv")], axis=0, ignore_index=True,join="outer")

path = path0 + "stthomas_stjohn_temps\\1.1\\data\\0-data\\NCRMP_WaterTemperatureData_STJOHN_STHOMAS_2014-2017\\NCRMP_STR_STTHOMAS_SOUTH\\"
stthomas_2 = pd.concat([pd.read_csv(f) for f in glob.glob(path+ "*.csv")], axis=0, ignore_index=True,join="outer")

path = path0 + "stthomas_stjohn_temps\\1.1\\data\\0-data\\NCRMP_WaterTemperatureData_STJOHN_STHOMAS_2014-2017\\NCRMP_STR_STTHOMAS_WEST\\"
stthomas_3 = pd.concat([pd.read_csv(f) for f in glob.glob(path+ "*.csv")], axis=0, ignore_index=True,join="outer")

stthomas = pd.concat([stthomas_1,stthomas_2,stthomas_3],axis=0,ignore_index=True,join="outer")
print(stthomas.shape)
print(stthomas.columns)
del stthomas_1,stthomas_2,stthomas_3, path


#Read in Timor-Leste data
timor_leste = pd.read_csv("timor_leste_temps\\1.1\\data\\0-data\\STR_Temperature_Timor_2014.csv")
print(timor_leste.shape)
print(timor_leste.columns)

#Read in Wake Island data
wake = pd.read_csv("wake_island_temps\\2.2\\data\\0-data\\unzipped\\ESD_NCRMP_Temperature_2014_PRIA.csv")
print(wake.shape)
print(wake.columns)


#Read in Puerto Rico data
path = path0 + "puerto_rico_temps\\1.1\\data\\0-data\\NCRMP_WaterTemperatureData_PuertoRico_2015-2017\\NCRMP_STR_PR_EAST\\"
pr_1 = pd.concat([pd.read_csv(f) for f in glob.glob(path+ "*.csv")], axis=0, ignore_index=True,join="outer")

path = path0 + "puerto_rico_temps\\1.1\\data\\0-data\\NCRMP_WaterTemperatureData_PuertoRico_2015-2017\\NCRMP_STR_PR_NORTH\\"
pr_2 = pd.concat([pd.read_csv(f) for f in glob.glob(path+ "*.csv")], axis=0, ignore_index=True,join="outer")

path = path0 + "puerto_rico_temps\\1.1\\data\\0-data\\NCRMP_WaterTemperatureData_PuertoRico_2015-2017\\NCRMP_STR_PR_SOUTH\\"
pr_3 = pd.concat([pd.read_csv(f) for f in glob.glob(path+ "*.csv")], axis=0, ignore_index=True,join="outer")

path = path0 + "puerto_rico_temps\\1.1\\data\\0-data\\NCRMP_WaterTemperatureData_PuertoRico_2015-2017\\NCRMP_STR_PR_WEST\\"
pr_4 = pd.concat([pd.read_csv(f) for f in glob.glob(path+ "*.csv")], axis=0, ignore_index=True,join="outer")

puerto_rico = pd.concat([pr_1,pr_2,pr_3,pr_4],axis=0,ignore_index=True,join="outer")
print(puerto_rico.shape)
print(puerto_rico.columns)

del pr_1,pr_2,pr_3,pr_4,path

#Read in Upper Florida Keys data
path = path0 + "upper_florida_keys_temps\\0126994.5.5\\0126994\\5.5\\data\\0-data\\"
upper_keys = pd.concat([pd.read_csv(f) for f in glob.glob(path+ "*.csv")], axis=0, ignore_index=True,join="outer")
print(upper_keys.shape)
print(upper_keys.columns)

#Read in other Florida Keys data
path = path0 + "florida_keys_temps\\1.1\\data\\0-data\\NCRMP_FloridaKeys_STR_Data_2013-12-03_to_2016-12-13\\NCRMP_STR_FL_KEYS_LOW\\"
keys_low =  pd.concat([pd.read_csv(f) for f in glob.glob(path+ "*.csv")], axis=0, ignore_index=True,join="outer")

path = path0 + "florida_keys_temps\\1.1\\data\\0-data\\NCRMP_FloridaKeys_STR_Data_2013-12-03_to_2016-12-13\\NCRMP_STR_FL_KEYS_MID\\"
keys_mid =  pd.concat([pd.read_csv(f) for f in glob.glob(path+ "*.csv")], axis=0, ignore_index=True,join="outer")

path = path0 + "florida_keys_temps\\1.1\\data\\0-data\\NCRMP_FloridaKeys_STR_Data_2013-12-03_to_2016-12-13\\NCRMP_STR_FL_KEYS_UPP\\"
keys_upp =  pd.concat([pd.read_csv(f) for f in glob.glob(path+ "*.csv")], axis=0, ignore_index=True,join="outer")

florida_keys = pd.concat([keys_low,keys_mid,keys_upp],axis=0,ignore_index=True,join="outer")
print(florida_keys.shape)
print(florida_keys.columns)

del keys_low,keys_mid,keys_upp,path

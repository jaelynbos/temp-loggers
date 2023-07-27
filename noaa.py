#!/usr/bin/env python
# coding: utf-8

import pandas as pd
import numpy as np
import glob
import geopandas as gpd
from datetime import datetime
from shapely.geometry import Point

'''
Define functions
'''
def make_point(df):
    df['COORDS'] = df[['LONGITUDE','LATITUDE']].values.tolist()
    df['COORDS'] = df['COORDS'].apply(Point)
    df_gpd = gpd.GeoDataFrame(df,geometry='COORDS')
    return(df_gpd)

def agg_and_merge(region,geo):
    di = {'LATITUDE':['first'],'LONGITUDE':'first'}
    reg_agg = region.groupby('ID').agg(di).reset_index()
    reg_agg.columns = ['ID','LATITUDE','LONGITUDE']
    reg_agg = make_point(reg_agg)
    reg_agg.crs = geo.crs
    reg = pd.DataFrame(gpd.sjoin(reg_agg,geo,how='inner',op='within'))
    region = pd.merge(reg,region,on='ID') 
    region['RCLASS'] = region['class']
    region['LONGITUDE'] = region['LONGITUDE_x']
    region['LATITUDE'] = region['LATITUDE_x']
    region = region.drop(columns=['LONGITUDE_x','LATITUDE_x','LONGITUDE_y','LATITUDE_y','index_right','class'])
    return(region)

def thirtyminsample(region,region_points):
    region = region.set_index('DATETIME')
    x = [region[region['ID'] == region_points[i]] for i in np.arange(0,len(region_points),1)]
    y = [x[i].resample('30T').first() for i in np.arange(0,len(x),1)]
    y = pd.concat(y)
    y = y.dropna(axis=0)
    return(y)

def downsampler (region):
    points_u = region['ID'].unique()
    region_ds = thirtyminsample(region,points_u)
    region_ds['DATETIME'] = region_ds.index
    region_ds['DATE'] = region_ds['DATETIME'].dt.date
    region_ds['YRMONTHPOINT'] = region_ds['DATETIME'].dt.year.astype('str') + '/' + region_ds['DATETIME'].dt.month.astype('str') +'_' + region_ds['ID'].astype('str')
    return(region_ds)

def sitesel(region,point):
    return(region.loc[region['ID']==point].reset_index())
    
def maxsel(site):
   return(site.loc[(site['TEMP_C']==site['TEMP_C'].max()).values])

def mmm_select(region,regional_points):
    return(pd.concat([maxsel(sitesel(region,regional_points[i])) for i in np.arange(0,len(regional_points),1)]).reset_index())

def quantile95(ser):
    return(ser.quantile(q=0.95))
    
def quantile98(ser):
    return(ser.quantile(q=0.98))
    
def prep_data(region_ds):
        region_ds['q95'] = region_ds['TEMP_C']
        region_ds['q98'] = region_ds['TEMP_C']
        region_ds['dmin'] = region_ds.groupby(['ID','DATE']).TEMP_C.transform('min')
        region_ds['dmax'] = region_ds.groupby(['ID','DATE']).TEMP_C.transform('max')
        region_ds['DAILY_DIFF'] = region_ds['dmax'] - region_ds['dmin']
        region_ds = region_ds.drop(columns=['dmin','dmax'])
        points_u= pd.Series(region_ds['ID'].unique())
        a = {'q95':[quantile95],'q98':[quantile98],'DAILY_DIFF': ['median'],'TEMP_C': ['mean'],'LONGITUDE':['median'],'LATITUDE':['median'],'RCLASS':['first'],'COORDS':['first']}
        mms = region_ds.groupby(['MONTH','ID']).agg(a).reset_index() 
        MMMs = mmm_select(mms,points_u)
        return(MMMs)
        
def prep_data_more(region_ds,region_mmms):    
        starts = region_ds.groupby(['ID'])['DATETIME'].min().to_frame().reset_index()
        starts.columns = ['ID','START_TIME']
        ends = region_ds.groupby(['ID'])['DATETIME'].max().to_frame().reset_index()
        ends.columns = ['ID','END_TIME']
        pts = region_mmms.copy()
        pts = pd.merge(pts,starts)
        pts = pd.merge(pts,ends)
        pts = pts.reset_index()
        return(pts)

path = '/projects/f_mlp195/bos_microclimate/loggers_training/NOAA_data/'
keepcols = ['LATITUDE','LONGITUDE','ID','DATETIME','TEMP_C']
'''
Hawaii
'''
hawaii_1 = pd.read_csv(path + "hawaii_temps/hawaii_temps_1/2.2/data/0-data/unzipped/ESD_NCRMP_Temperature_2013_Hawaii.csv")
hawaii_2 = pd.read_csv(path + "hawaii_temps/hawaii_temps_2/ESD_NCRMP_Temperature_2016_Hawaii/ESD_NCRMP_Temperature_2016_Hawaii.csv")
hawaii_3 = pd.read_csv(path + "hawaii_temps/hawaii_temps_3/ESD_NCRMP_Temperature_2019_Hawaii/ESD_NCRMP_Temperature_2019_Hawaii.csv")
hawaii = pd.concat([hawaii_1,hawaii_2,hawaii_3],axis=0,ignore_index=True,join="outer")
hawaii['DATETIME'] = pd.to_datetime(hawaii['TIMESTAMP'], format='%Y-%m-%d %H:%M:%S')
hawaii = hawaii.rename(columns={'OCC_SITEID':'ID'})
hawaii = hawaii[hawaii['DATETIME'] > pd.to_datetime(hawaii['VALIDDATASTART'])]
hawaii = hawaii[hawaii['DATETIME'] < pd.to_datetime(hawaii['VALIDDATAEND'])]
hawaii = hawaii[keepcols]
del hawaii_1,hawaii_2,hawaii_3
geom = gpd.read_file('/projects/f_mlp195/bos_microclimate/geomorphology/Hawaiian-Islands.gpkg')
hawaii = downsampler(agg_and_merge(hawaii,geom))

'''
West Hawaii (from USGS)
'''
hawaii_1 = pd.read_csv('/projects/f_mlp195/bos_microclimate/loggers_training/USGS_data/west_hawaii_temps/WaterTempTimeSeries_KAHO-KC.csv')
hawaii_2 = pd.read_csv('/projects/f_mlp195/bos_microclimate/loggers_training/USGS_data/west_hawaii_temps/WaterTempTimeSeries_KAHO.csv')
hawaii_3 = pd.read_csv('/projects/f_mlp195/bos_microclimate/loggers_training/USGS_data/west_hawaii_temps/WaterTempTimeSeries_westHawaii.csv')
west_hawaii = pd.concat([hawaii_1,hawaii_2,hawaii_3])
west_hawaii = west_hawaii.rename(columns = {'Site':'ID'})
west_hawaii = west_hawaii.rename(columns = {'Temperature_C':'TEMP_C'})
west_hawaii = west_hawaii.rename(columns = {'Latitude':'LATITUDE'})
west_hawaii = west_hawaii.rename(columns = {'Longitude':'LONGITUDE'})
west_hawaii['DATETIME'] = pd.to_datetime(west_hawaii['DateTime'])
west_hawaii = west_hawaii[keepcols]
del hawaii_1,hawaii_2,hawaii_3
west_hawaii = downsampler(agg_and_merge(west_hawaii,geom))

del geom

noaa_hawaii = pd.concat([hawaii,west_hawaii])
'''
Remove points with <12 months
'''
noaa_hawaii['MONTH'] =noaa_hawaii['DATETIME'].dt.month
d_agg = noaa_hawaii.groupby(['ID','MONTH']).first().reset_index()

monthn= pd.DataFrame(d_agg['ID'].value_counts())
monthn['counts'] =monthn.values
monthn['ID'] =monthn.index
monthn = monthn.reset_index()

noaa_hawaii = pd.merge(noaa_hawaii,monthn,how='left',on='ID')
noaa_hawaii = noaa_hawaii[noaa_hawaii['counts'] > 11]
noaa_hawaii = noaa_hawaii.reset_index(drop=True)

'''
Calculate MMMs
'''
hawaii_mmms = prep_data(noaa_hawaii)
hawaii_mmms.columns = ['level_0','index','MONTH','ID','q95','q98','DAILY_DIFF','TEMP_C','LONGITUDE','LATITUDE','RCLASS','COORDS']
hawaii_mmms = hawaii_mmms.drop(columns=['level_0','index'])

noaa_hawaii = prep_data_more(noaa_hawaii,hawaii_mmms)
outfile = open('/home/jtb188/noaa_hawaii.csv','wb')
noaa_hawaii.to_csv(outfile)
outfile.close()
del d_agg, monthn, noaa_hawaii

'''
American Samoa
'''
amsam = pd.read_csv(path + 'american_samoa_temps/2.2/data/0-data/unzipped/ESD_NCRMP_Temperature_2015_AMSM.csv')
amsam = amsam.rename(columns={'OCC_SITEID':'ID'})
amsam['DATETIME'] = pd.to_datetime(amsam['TIMESTAMP'],infer_datetime_format=True)
amsam = amsam[amsam['DATETIME'] > pd.to_datetime(amsam['VALIDDATASTART'])]
amsam = amsam[amsam['DATETIME'] < pd.to_datetime(amsam['VALIDDATAEND'])]
amsam = amsam[keepcols]
geom = gpd.read_file('/projects/f_mlp195/bos_microclimate/geomorphology/Southwestern-Pacific_E.gpkg')
geom = geom.rename(columns={'rclass':'class'})
amsam = downsampler(agg_and_merge(amsam,geom))
del geom
'''
Batangas
'''
batangas = pd.read_csv(path+ 'batangas_philippines_temps/1.1/data/0-data/MV_OCN_STR_TIMESERIES_Philippines.csv')
batangas = batangas.rename(columns={'TEMPERATURE':'TEMP_C'})
dummy = batangas['YEAR'].astype(str)+ batangas['MONTH'].astype(str).str.zfill(2) + batangas['DAY'].astype(str).str.zfill(2) +batangas['HOUR'].astype(str).str.zfill(2) +batangas['MINUTE'].astype(str).str.zfill(2)+batangas['SECOND'].astype(str).str.zfill(2) 
batangas['DATETIME'] = pd.to_datetime(dummy,format='%Y%m%d%H%M%S')
batangas = batangas.rename(columns={'SITETAG':'ID'})
batangas = batangas[batangas['DATETIME'] > pd.to_datetime(batangas['VALIDDATASTART'])]
batangas = batangas[batangas['DATETIME'] < pd.to_datetime(batangas['VALIDDATAEND'])]
batangas = batangas[keepcols]
geom = gpd.read_file('/projects/f_mlp195/bos_microclimate/geomorphology/Philippines.gpkg')
batangas = downsampler(agg_and_merge(batangas,geom))
del geom
'''
Marianas Islands
'''
marianas = pd.read_csv(path + "marianas_temps/ESD_NCRMP_Temperature_2017_Marianas.csv")
marianas['DATETIME'] = pd.to_datetime(marianas['TIMESTAMP'], format='%Y-%m-%d %H:%M:%S')
marianas = marianas.rename(columns={'OCC_SITEID':'ID'})
marianas = marianas[marianas['DATETIME'] > pd.to_datetime(marianas['VALIDDATASTART'])]
marianas = marianas[marianas['DATETIME'] < pd.to_datetime(marianas['VALIDDATAEND'])]
marianas = marianas[keepcols]
geom = gpd.read_file('/projects/f_mlp195/bos_microclimate/geomorphology/Western-Micronesia.gpkg')
marianas = downsampler(agg_and_merge(marianas,geom))
del geom
'''
Wake Island
'''
wake = pd.read_csv(path+ "wake_island_temps/2.2/data/0-data/unzipped/ESD_NCRMP_Temperature_2014_PRIA.csv")
wake['DATETIME'] = pd.to_datetime(wake['TIMESTAMP'])
wake = wake.rename(columns={'OCC_SITEID':'ID'})
wake = wake[wake['DATETIME'] > pd.to_datetime(wake['VALIDDATASTART'])]
wake = wake[wake['DATETIME'] < pd.to_datetime(wake['VALIDDATAEND'])]
wake = wake[keepcols]
geom = gpd.read_file('/projects/f_mlp195/bos_microclimate/geomorphology/Eastern-Micronesia_W.gpkg')
geom = geom.rename(columns={'rclass':'class'})
wake = downsampler(agg_and_merge(wake,geom))
del geom
'''
Pacific Remote Island Areas
'''
path1 = path + "pria_temps/pria_temps/pria_temps_1/"
pria_1 = ([pd.read_csv(f) for f in glob.glob(path1+ "*.csv")])
pria_1 = pd.concat(pria_1,axis=0,ignore_index=True,join="outer")
pria_1 = pria_1.rename(columns={'SITETAG':'ID'})
path2 = path + "pria_temps/pria_temps/pria_temps_2/2.2/data/0-data/unzipped/split/"
pria_2 = ([pd.read_csv(f) for f in glob.glob(path2+ "*.csv")])
pria_2 = pd.concat(pria_2,axis=0,ignore_index=True,join="outer")
pria_2 = pria_2.rename(columns={'OCC_SITEID':'ID'})
pria = pd.concat([pria_1,pria_2], axis=0, ignore_index=True,join="outer")
del pria_1,pria_2
pria['DATETIME'] = pd.to_datetime(pria['TIMESTAMP'])
pria = pria[pria['DATETIME'] > pd.to_datetime(pria['VALIDDATASTART'])]
pria = pria[pria['DATETIME'] < pd.to_datetime(pria['VALIDDATAEND'])]
pria = pria[keepcols]
geom1 = pd.DataFrame(gpd.read_file('/projects/f_mlp195/bos_microclimate/geomorphology/Eastern-Micronesia_E.gpkg'))
geom1 = geom1.rename(columns={'rclass':'class'})
geom2 = pd.DataFrame(gpd.read_file('/projects/f_mlp195/bos_microclimate/geomorphology/Eastern-Micronesia_W.gpkg'))
geom2 = geom2.rename(columns={'rclass':'class'})
geom3 = pd.DataFrame(gpd.read_file('/projects/f_mlp195/bos_microclimate/geomorphology/Hawaiian-Islands.gpkg'))
geoms = gpd.GeoDataFrame(pd.concat([geom1,geom2,geom3]))
pria = downsampler(agg_and_merge(pria,geoms))

'''
Timor Leste
'''
#Read in Timor-Leste data
timor_leste = pd.read_csv(path+ "timor_leste_temps/1.1/data/0-data/STR_Temperature_Timor_2014.csv")
dummy = timor_leste['_DATE'].astype(str)+ "_" + timor_leste['HOUR'].astype(str).str.zfill(2)
timor_leste['DATETIME'] = pd.to_datetime(dummy,format='%m/%d/%Y_%H')
timor_leste = timor_leste.rename(columns={'SITE_TAG':'ID'})
timor_leste = timor_leste[timor_leste['DATETIME'] > pd.to_datetime(timor_leste['DATA_START'])]
timor_leste = timor_leste[timor_leste['DATETIME'] < pd.to_datetime(timor_leste['DATA_END'])]
timor_leste = timor_leste[keepcols]
del dummy
geom = gpd.read_file('/projects/f_mlp195/bos_microclimate/geomorphology/Southeast-Asian-Archipelago.gpkg')
timor_leste = downsampler(agg_and_merge(timor_leste,geom))

noaa_pacific = pd.concat([amsam,batangas,marianas,wake,pria,timor_leste])

'''
Remove points with <12 months
'''
noaa_pacific['MONTH'] =noaa_pacific['DATETIME'].dt.month
d_agg = noaa_pacific.groupby(['ID','MONTH']).first().reset_index()

monthn= pd.DataFrame(d_agg['ID'].value_counts())
monthn['counts'] =monthn.values
monthn['ID'] =monthn.index
monthn = monthn.reset_index()

noaa_pacific = pd.merge(noaa_pacific,monthn,how='left',on='ID')
noaa_pacific = noaa_pacific[noaa_pacific['counts'] > 11]
noaa_pacific = noaa_pacific.reset_index(drop=True)

'''
Calculate MMMs
'''
pacific_mmms = prep_data(noaa_pacific)
pacific_mmms.columns = ['level_0','index','MONTH','ID','q95','q98','DAILY_DIFF','TEMP_C','LONGITUDE','LATITUDE','RCLASS','COORDS']
pacific_mmms = pacific_mmms.drop(columns=['level_0','index'])

noaa_pacific = prep_data_more(noaa_pacific,pacific_mmms)
outfile = open('/home/jtb188/noaa_pacific.csv','wb')
noaa_pacific.to_csv(outfile)
outfile.close()
del d_agg, monthn, noaa_pacific

''' 
Biscayne Bay
'''
bpath = path + "biscayne_florida_temps/1.1/data/0-data/NCRMP_WaterTemperatureData_BiscayneNationalPark_2014-2017/NCRMP_STR_BNP/"
biscayne = pd.concat([pd.read_csv(f) for f in glob.glob(bpath+ "*.csv")], axis=0, ignore_index=True,join="outer")
biscayne['DATETIME'] = pd.to_datetime(biscayne['TIMESTAMP'],format='%m/%d/%Y %H:%M:%S')
biscayne = biscayne.rename(columns={'SITETAG':'ID'})
biscayne = biscayne[biscayne['DATETIME'] > pd.to_datetime(biscayne['VALIDDATASTART'])]
biscayne = biscayne[biscayne['DATETIME'] < pd.to_datetime(biscayne['VALIDDATAEND'])]
biscayne = biscayne[keepcols]
geom = gpd.read_file('/projects/f_mlp195/bos_microclimate/geomorphology/Northern-Caribbean-Florida-Bahamas.gpkg')
biscayne = downsampler(agg_and_merge(biscayne,geom))

'''
Florida Keys
'''
path1 = path + "florida_keys_temps/1.1/data/0-data/NCRMP_FloridaKeys_STR_Data_2013-12-03_to_2016-12-13/NCRMP_STR_FL_KEYS_LOW/"
keys_low =  pd.concat([pd.read_csv(f) for f in glob.glob(path1+ "*.csv")], axis=0, ignore_index=True,join="outer")
path2 = path + "florida_keys_temps/1.1/data/0-data/NCRMP_FloridaKeys_STR_Data_2013-12-03_to_2016-12-13/NCRMP_STR_FL_KEYS_MID/"
keys_mid =  pd.concat([pd.read_csv(f) for f in glob.glob(path2+ "*.csv")], axis=0, ignore_index=True,join="outer")
path3 = path + "florida_keys_temps/1.1/data/0-data/NCRMP_FloridaKeys_STR_Data_2013-12-03_to_2016-12-13/NCRMP_STR_FL_KEYS_UPP/"
keys_upp =  pd.concat([pd.read_csv(f) for f in glob.glob(path3+ "*.csv")], axis=0, ignore_index=True,join="outer")
florida_keys = pd.concat([keys_low,keys_mid,keys_upp],axis=0,ignore_index=True,join="outer")
dummy = florida_keys['TIMESTAMP'] + "_" + florida_keys['HOUR']
florida_keys['DATETIME'] = pd.to_datetime(dummy,format='%m/%d/%y_%H:%M:%S',errors='coerce')
florida_keys = florida_keys.rename(columns={'SITETAG':'ID'})
florida_keys = florida_keys[florida_keys['DATETIME'] > pd.to_datetime(florida_keys['VALIDDATASTART'])]
florida_keys = florida_keys[florida_keys['DATETIME'] < pd.to_datetime(florida_keys['VALIDDATAEND'])]
florida_keys = florida_keys[keepcols]
del keys_low,keys_mid,keys_upp,dummy
florida_keys = downsampler(agg_and_merge(florida_keys,geom))

'''
St Johns
'''
path1 = path + "stthomas_stjohn_temps/1.1/data/0-data/NCRMP_WaterTemperatureData_STJOHN_STHOMAS_2014-2017/NCRMP_STR_STJOHN_EAST/"
stjohn_1 = pd.concat([pd.read_csv(f) for f in glob.glob(path1+ "*.csv")], axis=0, ignore_index=True,join="outer")
path2 = path + "stthomas_stjohn_temps/1.1/data/0-data/NCRMP_WaterTemperatureData_STJOHN_STHOMAS_2014-2017/NCRMP_STR_STJOHN_NORTH/"
stjohn_2 = pd.concat([pd.read_csv(f) for f in glob.glob(path2+ "*.csv")], axis=0, ignore_index=True,join="outer")
path3 = path + "stthomas_stjohn_temps/1.1/data/0-data/NCRMP_WaterTemperatureData_STJOHN_STHOMAS_2014-2017/NCRMP_STR_STJOHN_SOUTH/"
stjohn_3 = pd.concat([pd.read_csv(f) for f in glob.glob(path3+ "*.csv")], axis=0, ignore_index=True,join="outer")
path4 = path + "stthomas_stjohn_temps/1.1/data/0-data/NCRMP_WaterTemperatureData_STJOHN_STHOMAS_2014-2017/NCRMP_STR_STJOHN_WEST/"
stjohn_4 = pd.concat([pd.read_csv(f) for f in glob.glob(path4 + "*.csv")], axis=0, ignore_index=True,join="outer")
stjohn = pd.concat([stjohn_1,stjohn_2,stjohn_3,stjohn_4],axis=0,ignore_index=True,join="outer")
stjohn['DATETIME'] = pd.to_datetime(stjohn['TIMESTAMP'], format='%m/%d/%Y %H:%M:%S')
stjohn = stjohn.rename(columns={'SITETAG':'ID'})
stjohn = stjohn[stjohn['DATETIME'] > pd.to_datetime(stjohn['VALIDDATASTART'])]
stjohn = stjohn[stjohn['DATETIME'] < pd.to_datetime(stjohn['VALIDDATAEND'])]
stjohn = stjohn[keepcols]
del stjohn_1,stjohn_2,stjohn_3,stjohn_4
geom = gpd.read_file('/projects/f_mlp195/bos_microclimate/geomorphology/Southeastern-Caribbean.gpkg')
stjohn = downsampler(agg_and_merge(stjohn,geom))

'''
St Thomas
'''
path1 = path + "stthomas_stjohn_temps/1.1/data/0-data/NCRMP_WaterTemperatureData_STJOHN_STHOMAS_2014-2017/NCRMP_STR_STTHOMAS_NORTH/"
stthomas_1 = pd.concat([pd.read_csv(f) for f in glob.glob(path1+ "*.csv")], axis=0, ignore_index=True,join="outer")
path2 = path + "stthomas_stjohn_temps/1.1/data/0-data/NCRMP_WaterTemperatureData_STJOHN_STHOMAS_2014-2017/NCRMP_STR_STTHOMAS_SOUTH/"
stthomas_2 = pd.concat([pd.read_csv(f) for f in glob.glob(path2+ "*.csv")], axis=0, ignore_index=True,join="outer")
path3 = path + "stthomas_stjohn_temps/1.1/data/0-data/NCRMP_WaterTemperatureData_STJOHN_STHOMAS_2014-2017/NCRMP_STR_STTHOMAS_WEST/"
stthomas_3 = pd.concat([pd.read_csv(f) for f in glob.glob(path3+ "*.csv")], axis=0, ignore_index=True,join="outer")

stthomas = pd.concat([stthomas_1,stthomas_2,stthomas_3],axis=0,ignore_index=True,join="outer")
stthomas['DATETIME'] = pd.to_datetime(stthomas['TIMESTAMP'], format='%m/%d/%Y %H:%M:%S')
stthomas = stthomas.rename(columns={'SITETAG':'ID'})
stthomas = stthomas[stthomas['DATETIME'] > pd.to_datetime(stthomas['VALIDDATASTART'])]
stthomas = stthomas[stthomas['DATETIME'] < pd.to_datetime(stthomas['VALIDDATAEND'])]
stthomas = stthomas[keepcols]
del stthomas_1,stthomas_2,stthomas_3
stthomas = downsampler(agg_and_merge(stthomas,geom))

'''
Puerto Rico
'''
path1 = path + "puerto_rico_temps/1.1/data/0-data/NCRMP_WaterTemperatureData_PuertoRico_2015-2017/NCRMP_STR_PR_EAST/"
pr_1 = pd.concat([pd.read_csv(f) for f in glob.glob(path1+ "*.csv")], axis=0, ignore_index=True,join="outer")
path2 = path + "puerto_rico_temps/1.1/data/0-data/NCRMP_WaterTemperatureData_PuertoRico_2015-2017/NCRMP_STR_PR_NORTH/"
pr_2 = pd.concat([pd.read_csv(f) for f in glob.glob(path2+ "*.csv")], axis=0, ignore_index=True,join="outer")
path3 = path + "puerto_rico_temps/1.1/data/0-data/NCRMP_WaterTemperatureData_PuertoRico_2015-2017/NCRMP_STR_PR_SOUTH/"
pr_3 = pd.concat([pd.read_csv(f) for f in glob.glob(path3+ "*.csv")], axis=0, ignore_index=True,join="outer")
path4 = path + "puerto_rico_temps/1.1/data/0-data/NCRMP_WaterTemperatureData_PuertoRico_2015-2017/NCRMP_STR_PR_WEST/"
pr_4 = pd.concat([pd.read_csv(f) for f in glob.glob(path4+ "*.csv")], axis=0, ignore_index=True,join="outer")
puerto_rico = pd.concat([pr_1,pr_2,pr_3,pr_4],axis=0,ignore_index=True,join="outer")
puerto_rico['DATETIME'] = pd.to_datetime(puerto_rico['TIMESTAMP'],errors='coerce')
puerto_rico = puerto_rico.rename(columns={'SITETAG':'ID'})
puerto_rico = puerto_rico[puerto_rico['DATETIME'] > pd.to_datetime(puerto_rico['VALIDDATASTART'])]
puerto_rico = puerto_rico[puerto_rico['DATETIME'] < pd.to_datetime(puerto_rico['VALIDDATAEND'])]
puerto_rico = puerto_rico[keepcols]
del pr_1,pr_2,pr_3,pr_4
puerto_rico = downsampler(agg_and_merge(puerto_rico,geom))

'''
St Croix
'''
path1 = path + "stcroix_temps/stcroix_temps_2/2.2/data/0-data/NCRMP_STR_Data_STX_2019/"
stcroix_1 = ([pd.read_csv(f) for f in glob.glob(path1+ "*.csv")])
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

path2 = path + "stcroix_temps/stcroix_temps_1/1.1/data/0-data/NCRMP_STR_StCroix_WaterTemperatureData_2013-9-9_to_2016-9-14/NCRMP_STR_STCROIX_EAST/"
stcroix_2 = pd.concat([pd.read_csv(f) for f in glob.glob(path2+ "*.csv")], axis=0, ignore_index=True,join="outer")

path3 = path + "stcroix_temps/stcroix_temps_1/1.1/data/0-data/NCRMP_STR_StCroix_WaterTemperatureData_2013-9-9_to_2016-9-14/NCRMP_STR_STCROIX_NORTH/"
stcroix_3 = pd.concat([pd.read_csv(f) for f in glob.glob(path3+ "*.csv")], axis=0, ignore_index=True,join="outer")

path4 = path + "stcroix_temps/stcroix_temps_1/1.1/data/0-data/NCRMP_STR_StCroix_WaterTemperatureData_2013-9-9_to_2016-9-14/NCRMP_STR_STCROIX_SOUTH/"
stcroix_4 = pd.concat([pd.read_csv(f) for f in glob.glob(path4+ "*.csv")], axis=0, ignore_index=True,join="outer")

path5 = path + "stcroix_temps/stcroix_temps_1/1.1/data/0-data/NCRMP_STR_StCroix_WaterTemperatureData_2013-9-9_to_2016-9-14/NCRMP_STR_STCROIX_WEST/"
stcroix_5 = pd.concat([pd.read_csv(f) for f in glob.glob(path5+ "*.csv")], axis=0, ignore_index=True,join="outer")

stcroix = pd.concat([stcroix_1,stcroix_2,stcroix_3,stcroix_4,stcroix_5],axis=0,ignore_index=True,join="outer")
dummy = stcroix['TIMESTAMP'] + "_" + stcroix['HOUR']
stcroix['DATETIME'] = pd.to_datetime(dummy,format='%m/%d/%y_%H:%M:%S',errors='coerce')

stcroix = stcroix.rename(columns={'SITETAG':'ID'})
stcroix = stcroix[stcroix['DATETIME'] > pd.to_datetime(stcroix['VALIDDATASTART'])]
stcroix = stcroix[stcroix['DATETIME'] < pd.to_datetime(stcroix['VALIDDATAEND'])]
stcroix = stcroix[keepcols]
del stcroix_1,stcroix_2,stcroix_3,stcroix_4,stcroix_5,dummy

stcroix = downsampler(agg_and_merge(stcroix,geom))

noaa_caribbean = pd.concat([biscayne,florida_keys,stjohn,stthomas,puerto_rico,stcroix])
'''
Remove points with <12 months
'''
noaa_caribbean['MONTH'] =noaa_caribbean['DATETIME'].dt.month
d_agg = noaa_caribbean.groupby(['ID','MONTH']).first().reset_index()

monthn= pd.DataFrame(d_agg['ID'].value_counts())
monthn['counts'] =monthn.values
monthn['ID'] =monthn.index
monthn = monthn.reset_index()

noaa_caribbean = pd.merge(noaa_caribbean,monthn,how='left',on='ID')
noaa_caribbean = noaa_caribbean[noaa_caribbean['counts'] > 11]
noaa_caribbean = noaa_caribbean.reset_index(drop=True)

'''
Calculate MMMs
'''
caribbean_mmms = prep_data(noaa_caribbean)
caribbean_mmms.columns = ['level_0','index','MONTH','ID','q95','q98','DAILY_DIFF','TEMP_C','LONGITUDE','LATITUDE','RCLASS','COORDS']
caribbean_mmms = caribbean_mmms.drop(columns=['level_0','index'])

noaa_caribbean = prep_data_more(noaa_caribbean,caribbean_mmms)
outfile = open('/home/jtb188/noaa_caribbean.csv','wb')
noaa_caribbean.to_csv(outfile)
outfile.close()
del d_agg, monthn, noaa_caribbean
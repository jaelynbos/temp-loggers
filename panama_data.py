#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Mon Aug  2 11:06:02 2021

@author: jtb188
"""
import numpy as np
import pandas as pd
import geopandas as gpd
import xarray as xr
from shapely.geometry import Point

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
    
keepcols = ['LATITUDE','LONGITUDE','ID','DATETIME','TEMP_C']

path0 = '/projects/f_mlp195/bos_microclimate/loggers_testing/Panama_loggers/'


'''
San Blas, Bastimentos, and Mangrove Inn data were excluded from this analysis for being entirely before the year 2000
Isla Solarte exluded for being entirely before 2002
'''

bahia_lasminas = pd.read_csv(path0 + 'Bahia Las Minas_wt/Bahia Las Minas_03m_wt.csv')
bahia_lasminas['LATITUDE'] = 9.397833
bahia_lasminas['LONGITUDE'] = -79.82295

bocas = pd.read_csv(path0+ 'BocasPlatform/bocas_tower_wt_elect.csv')
bocas['LATITUDE'] = 9.35078
bocas['LONGITUDE'] = -82.257686

chiriqui_ji0 = pd.read_csv(path0+ 'Gulf_Chiriqui_Jicarita_wt/SST_cqjh_wt_elect.csv')
chiriqui_ji0['LATITUDE'] = 7.213639
chiriqui_ji0['LONGITUDE'] = -81.799167

chiriqui_ji1 = pd.read_csv(path0+ 'Gulf_Chiriqui_Jicarita_wt/SST_cqkh_wt_elect.csv')
chiriqui_ji1['LATITUDE'] = 7.225783
chiriqui_ji1['LONGITUDE'] = -81.829317

chiriqui_oc = pd.read_csv(path0+ 'Gulf_Chiriqui_OuterCanal_wt/SST_cqah_wt_elect.csv')
chiriqui_oc['LATITUDE'] = 7.225783
chiriqui_oc['LONGITUDE'] = -81.829317

chiriqui_ra = pd.read_csv(path0+ 'Gulf_Chiriqui_Rancheria_wt/SST_cqrh_wt_elect.csv')
chiriqui_ra['LATITUDE'] = 7.635250
chiriqui_ra['LONGITUDE'] = -81.697278

chiriqui_rp = pd.read_csv(path0+ 'Gulf_Chiriqui_RocaPropsera_wt/SST_cqph_wt_elect.csv')
chiriqui_rp['LATITUDE'] = 7.7763
chiriqui_rp['LONGITUDE'] = -81.758783

isla_coiba_rh = pd.read_csv(path0 + 'Isla_Coiba_RocaHacha_wt/SST_cqhh_wt_elect.csv')
isla_coiba_rh['LATITUDE'] = 7.431944
isla_coiba_rh['LONGITUDE'] = -81.858056

isla_coiba_fr = pd.read_csv(path0 + 'Isla_Coiba_Frijoles_wt/SST_cqfh_wt_elect.csv')
isla_coiba_fr['LATITUDE'] = 7.649889
isla_coiba_fr['LONGITUDE'] = -81.719278

isla_coiba_ca = pd.read_csv(path0 + 'Isla_Coiba_Catedral_wt/SST_cqch_wt_elect.csv')
isla_coiba_ca['LATITUDE'] = 7.226028
isla_coiba_ca['LONGITUDE'] = -81.829278

isla_coiba_ne = pd.read_csv(path0 + 'Isla_Coiba Noreste_ANAM_wt/SST_cbnh_wt_elect.csv')
isla_coiba_ne['LATITUDE'] = 9.133611
isla_coiba_ne['LONGITUDE'] = -82.040278

isla_coiba_id = pd.read_csv(path0 + 'Isla Coiba_Isla Damas_wt/SST_cbih_wt_elect.csv')
isla_coiba_id['LATITUDE'] = 7.421944
isla_coiba_id['LONGITUDE'] = -81.695556

isla_sanjose = pd.read_csv(path0 + 'Isla San Jose_wt/Isla San Jose, 04.5m_wt.csv')
isla_sanjose['LATITUDE'] = 8.256889
isla_sanjose['LONGITUDE'] = -79.133889

isla_pp= pd.read_csv(path0 + 'Isla Pedro-Pablo_wt/SST_ppah_wt_elect.csv')
isla_pp['LATITUDE'] = 8.459528
isla_pp['LONGITUDE'] = -78.852278

isla_parida= pd.read_csv(path0 + 'Isla Parida_wt/SST_pdah_wt_elect.csv')
isla_parida['LATITUDE'] = 8.131389
isla_parida['LONGITUDE'] = -82.320833

naos_tanks = pd.read_csv(path0 + 'Naos_SeawaterTable_wt/Naos, Seawater Tanks_wt.csv')
naos_tanks['LATITUDE'] = 8.9167
naos_tanks['LONGITUDE'] = -79.532822

naos_pier= pd.read_csv(path0 + 'Naos_Main Pier_wt/SST_naph_wt_elect.csv')
naos_pier['LATITUDE'] = 8.917494
naos_pier['LONGITUDE'] = -79.532842

galeta_ch = pd.read_csv(path0 + 'Galeta_Channel_03m_wt.csv')
galeta_dn = pd.read_csv(path0 + 'Galeta_Downstream_wt.csv')
galeta_up = pd.read_csv(path0 + 'Galeta_Upstream_wt.csv')

isla_grande = pd.read_csv(path0 + 'Isla Grande_03m_wt.csv')
isla_roldan = pd.read_csv(path0 + 'Isla Roldan_03m_wt.csv')
isla_taboguilla = pd.read_csv(path0 + 'Isla Taboguilla_12m_wt.csv')
isla_cayoagua = pd.read_csv(path0 + 'Isla_CayoAgua_03m_wt.csv')
isla_colon_mm = pd.read_csv(path0 + 'Isla_Colon_MangroveMud _wt.csv')
isla_colon_sg = pd.read_csv(path0 + 'Isla_Colon_Seagrass_02m_wt.csv')

panama0 = pd.concat([bahia_lasminas,bocas,chiriqui_ji0,chiriqui_ji1,chiriqui_oc,chiriqui_ra,chiriqui_rp,isla_coiba_rh,isla_coiba_fr,isla_coiba_ca,isla_coiba_ne,isla_coiba_id,isla_sanjose,isla_pp,isla_parida,naos_tanks,naos_tanks],keys=['bahia_lasminas','bocas','chiriqui_ji0','chiriqui_ji1','chiriqui_oc','chiriqui_ra','chiriqui_rp','isla_coiba_rh','isla_coiba_fr','isla_coiba_ca','isla_coiba_ne','isla_coiba_id','isla_sanjose','isla_pp','isla_parida','naos_tank','naos_tanks']).reset_index()
panama0['TEMP_C'] = panama0['wt'].apply(float)
panama0['ID'] = panama0['level_0']
panama0['DATETIME'] = pd.to_datetime(panama0['datetime'])
panama0 = panama0[panama0['chk_note'] !='missing']
panama0 = panama0.drop(columns=['level_0','level_1','datetime','date','wt','raw','chk_note','chk_fail','cid'])

'''
Add geomorphology from both Pacific and Caribbean side
'''
geom = gpd.read_file('/projects/f_mlp195/bos_microclimate/geomorphology/Eastern-Tropical-Pacific.gpkg')
panama_pacific = downsampler(agg_and_merge(panama0,geom))

geom = gpd.read_file('/projects/f_mlp195/bos_microclimate/geomorphology/Mesoamerica.gpkg')
panama_carib = downsampler(agg_and_merge(panama0,geom))

panama = pd.concat([panama_pacific,panama_carib])

'''
Remove data from before 2003
'''
panama = panama[panama['DATETIME'] > pd.to_datetime('2003-01-01 00:00:00')]
panama = panama.reset_index(drop=True)

panama['MONTH'] =panama['DATETIME'].dt.month
d_agg = panama.groupby(['ID','MONTH']).first().reset_index()

'''
Remove points with <12 months
'''

monthn= pd.DataFrame(d_agg['ID'].value_counts())
monthn['counts'] =monthn.values
monthn['ID'] =monthn.index
monthn = monthn.reset_index()

panama = pd.merge(panama,monthn,how='left',on='ID')
panama = panama[panama['counts'] > 11]
panama = panama.reset_index(drop=True)

panama_mmms = prep_data(panama)
panama_mmms.columns = ['level_0','index','MONTH','ID','q95','q98','DAILY_DIFF','TEMP_C','LONGITUDE','LATITUDE','RCLASS','COORDS']
panama_mmms = panama_mmms.drop(columns=['level_0','index'])

panama = prep_data_more(panama,panama_mmms)

outfile = open('/home/jtb188/panama.csv','wb')
panama.to_csv(outfile)
outfile.close()

#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Mon Apr  5 14:44:50 2021

@author: jtb188
"""
import pandas as pd
import numpy as np
import glob
import seaborn as sns
import matplotlib.pyplot as plt

geopath = '/home/jtb188/Documents/allen_coral_atlas/agg_merge/'
geo_data = [pd.read_pickle(f) for f in sorted(glob.glob(geopath + "*pkl"))]
geo_data[1]['TIMESTAMP'].isnull().value_counts()
geo_data[1] = geo_data[1].dropna(axis=0)

##################################################################
#Time interval checks
##################################################################
for i in np.arange(0,len(geo_data),1):
    geo_data[i]['DATE'] = geo_data[i]['TIMESTAMP'].dt.date
    
geo_agg = [None]*len(geo_data)
for i in np.arange(0,len(geo_agg),1):
    geo_agg[i] = geo_data[i].groupby(['DATE','POINTS']).first().reset_index()
    
points_u = [None]*len(geo_agg)
for i in np.arange(0,len(geo_agg),1):
    points_u[i] = pd.Series(geo_agg[i]['POINTS'].unique())

for i in np.arange(0,len(geo_agg),1):
    geo_agg[i]['YRMONTH'] = geo_agg[i]['TIMESTAMP'].dt.year.astype('str') + '/' + geo_agg[i]['TIMESTAMP'].dt.month.astype('str')
    
def monthsinsite(site):
    mo = site['YRMONTH'].unique()
    return([site[site['YRMONTH']==mo[i]] for i in np.arange(0,len(mo),1)])
        
def daysinmonth(site_month):
    x = len(site_month['DATE'].unique())
    if x > 27: 
        return (True)
    else:
        return (False)

def ntrue(point):
    y = pd.Series([daysinmonth(monthsinsite(point)[i]) for i in np.arange(0,len(monthsinsite(point)),1)])
    return(y.value_counts())

def complete_months(df,points_u):
    cm = [ntrue(df[df['POINTS'] == points_u[i]]) for i in np.arange(0,len(points_u),1)]
    return(cm)

def time_step(logger):
    logger = logger.sort_values(by=['TIMESTAMP']).reset_index()
    ts = [logger['TIMESTAMP'][i+1] - logger['TIMESTAMP'][i] for i in np.arange(0,(len(logger)-1),1)]
    ts = (pd.Series(ts).astype('timedelta64[m]'))
    #x = sns.distplot(ts)
    #x.set(xlim=(0,60))
    #return(x)
    return(['median =', np.median(ts), 'mean =', np.mean(ts),'min=', min(ts),'max = ', max(ts)])

##################################################################
#
###################################################################

###################################################################
#Downsample to every 30 minutes
###################################################################
points_u = [None]*len(geo_data)
for i in np.arange(0,len(geo_data),1):
    points_u[i]= geo_data[i]['POINTS'].unique()

def thirtyminsample(region,region_points):
    region = region.set_index('TIMESTAMP')
    x = [region[region['POINTS'] == region_points[i]] for i in np.arange(0,len(region_points),1)]
    y = [x[i].resample('30T').first() for i in np.arange(0,len(x),1)]
    return(pd.concat(y))

geo_downsample = [thirtyminsample(geo_data[j],points_u[j]) for j in np.arange(0,len(geo_data),1)]

os.chdir('/home/jtb188/Documents/')

coralsea_ds = geo_downsample[0]
coralsea_ds.to_pickle('coralsea_ds.pkl')
flkeys_ds = geo_downsample[1]
flkeys_ds.to_pickle('flkeys_ds.pkl')
gbr_ds = pd.concat([geo_downsample[2],geo_downsample[3],geo_downsample[4]])
gbr_ds.to_pickle('gbr_ds.pkl')
hawaii_ds = geo_downsample[5]
hawaii_ds.to_pickle('hawaii_ds.pkl')
solomon_ds = geo_downsample[6]
solomon_ds.to_pickle('solomon_ds.pkl')
swpacific_ds = geo_downsample[7]
swpacific_ds.to_pickle('swpacific_ds.pkl')
timorsea_ds = geo_downsample[8]
timorsea_ds.to_pickle('timorsea_ds.pkl')
westaus_ds = geo_downsample[9]
westaus_ds.to_pickle('westaus_ds.pkl')

####################################################################
#Complete months
###################################################################

###################################################################
#MMMs
###################################################################
dspath = '/home/jtb188/Documents/'
geo_downsample = [pd.read_pickle(f) for f in sorted(glob.glob(dspath+ "*.pkl"))]

points_u = [None]*len(geo_downsample)

for i in np.arange(0,len(geo_downsample),1):
    points_u[i] = pd.Series(geo_downsample[i]['POINTS'].unique())

for i in np.arange(0,len(geo_downsample),1):
    geo_downsample[i]['TIMESTAMP'] = geo_downsample[i].index
    
for i in np.arange(0,len(geo_downsample),1):
    geo_downsample[i]['MONTH'] = geo_downsample[i]['TIMESTAMP'].dt.month

for i in np.arange(0,len(geo_downsample),1):
    geo_downsample[i]['DAYS'] = geo_downsample[i]['TIMESTAMP'].dt.date

for i in np.arange(0,len(geo_downsample),1):
    geo_downsample[i]['dmin'] = geo_downsample[i].groupby(['POINTS','DAYS']).TEMP_C.transform('min')
    geo_downsample[i]['dmax'] = geo_downsample[i].groupby(['POINTS','DAYS']).TEMP_C.transform('max')
    geo_downsample[i]['DAILY_DIFF'] = geo_downsample[i]['dmax'] - geo_downsample[i]['dmin']
    geo_downsample[i] = geo_downsample[i].drop(columns=['dmin','dmax'])

for i in np.arange(0,len(geo_downsample),1):
    geo_downsample[i]['RCLASS'] = geo_downsample[i]['class']
    geo_downsample[i] = geo_downsample[i].drop(columns=['class'])
        
a = {'DAILY_DIFF': ['median'],'TEMP_C': ['mean'],'LONGITUDE':['median'],'LATITUDE':['median'],'DEPTH':['median'],'RCLASS':['first'],'COORDS':['first']}
mms = [geo_downsample[i].groupby(['MONTH','POINTS']).agg(a).reset_index() for i in np.arange(0,len(geo_downsample),1)]

def sitesel(region,point):
    return(region.loc[region['POINTS']==point].reset_index())
    
def maxsel(site):
   return(site.loc[(site['TEMP_C']==site['TEMP_C'].max()).values])

def mmm_select(region,regional_points):
    return(pd.concat([maxsel(sitesel(region,regional_points[i])) for i in np.arange(0,len(regional_points),1)]).reset_index())

MMMs = [mmm_select(mms[i],points_u[i]) for i in np.arange(0,len(mms),1)]

for i in np.arange(0,len(MMMs),1):
    MMMs[i] = MMMs[i].drop(columns='level_0')
    MMMs[i] = MMMs[i].reset_index()
    MMMs[i].columns = ['level_0','index','MONTH','POINTS','DAILY_DIFF','TEMP_C','LONGITUDE','LATITUDE','DEPTH','RCLASS','COORDS']
    MMMs[i] = pd.DataFrame(MMMs[i])

###############################################################
#Some boxplots
##############################################################
rclass_ord = ['Terrestrial Reef Flat','Shallow Lagoon','Deep Lagoon','Back Reef Slope','Inner Reef Flat','Outer Reef Flat','Reef Crest','Sheltered Reef Slope','Reef Slope','Plateau']
rclass_col = {'Terrestrial Reef Flat':'pink','Shallow Lagoon':'dodgerblue','Deep Lagoon':'mediumblue','Back Reef Slope':'c','Inner Reef Flat':'orchid','Outer Reef Flat':'mediumpurple','Reef Crest':'purple','Sheltered Reef Slope':'darkgreen','Reef Slope':'seagreen','Plateau':'darkorange'}
boxes = sns.boxplot(y='TEMP_C',x='RCLASS',data=MMMs[2],order=rclass_ord,hue='RCLASS',palette=rclass_col,dodge=False)
boxes.set_ylabel('MMMs')
boxes.set_title('GBR MMMs by reef class (n=181)')
boxes.get_legend().remove()
plt.xticks(rotation=80)

boxes = sns.boxplot(y='TEMP_C',x='RCLASS',data=MMMs[3],order=rclass_ord,hue='RCLASS',palette=rclass_col,dodge=False)
boxes.set_ylabel('MMM')
boxes.set_title('Hawaii MMMs by reef class (n=86)')
boxes.get_legend().remove()
plt.xticks(rotation=80)

boxes = sns.boxplot(y='TEMP_C',x='RCLASS',data=MMMs[5],order=rclass_ord,hue='RCLASS',palette=rclass_col,dodge=False)
boxes.set_ylabel('MMM')
boxes.set_title('American Samoa MMMs by reef class (n=18)')
boxes.get_legend().remove()
plt.xticks(rotation=80)

boxes = sns.boxplot(y='TEMP_C',x='RCLASS',data=MMMs[6],order=rclass_ord,hue='RCLASS',palette=rclass_col,dodge=False)
boxes.set_ylabel('MMM')
boxes.set_title('North Australia MMMs by reef class (n=33)')
boxes.get_legend().remove()
plt.xticks(rotation=80)

boxes = sns.boxplot(y='TEMP_C',x='RCLASS',data=MMMs[7],order=rclass_ord,hue='RCLASS',palette=rclass_col,dodge=False)
boxes.set_ylabel('MMM')
boxes.set_title('West Australia MMMs by reef class (n=25)')
boxes.get_legend().remove()
plt.xticks(rotation=80)

boxes = sns.boxplot(y='TEMP_C',x='RCLASS',data=MMMs[1],order=rclass_ord,hue='RCLASS',palette=rclass_col,dodge=False)
boxes.set_ylabel('MMM')
boxes.set_title('Florida MMMs by reef class (n=8)')
boxes.get_legend().remove()
plt.xticks(rotation=80)

Aus_MMMs = pd.concat([MMMs[0],MMMs[2],MMMs[6],MMMs[7]])

boxes = sns.boxplot(y='TEMP_C',x='RCLASS',data=Aus_MMMs,order=rclass_ord,hue='RCLASS',palette=rclass_col,dodge=False)
boxes.set_ylabel('MMM')
boxes.set_title('All Australia MMMs by reef class (n=270)')
boxes.get_legend().remove()
plt.xticks(rotation=80)

boxes = sns.boxplot(y='DEPTH',x='RCLASS',data=Aus_MMMs,order=rclass_ord,hue='RCLASS',palette=rclass_col,dodge=False)
boxes.set_ylabel('LOGGER DEPTH IN M')
boxes.set_title('All Australia depth by reef class (n=270)')
boxes.get_legend().remove()
plt.xticks(rotation=80)

#######################################################
# Some correlations and scatterplots
#######################################################

#GBR MMMs x dailydiff
Aus_MMMs.corr(method='pearson')['TEMP_C']
scat = sns.scatterplot(y='DAILY_DIFF',x='DEPTH',data=Aus_MMMs)

scat = sns.scatterplot(y='TEMP_C',x='DEPTH',data=Aus_MMMs)
scat.set_ylabel('MMM in degrees C')
scat.set_xlabel('Logger depth in meters')
scat.set_title('Depth vs MMM all Australia')

scat = sns.scatterplot(y='TEMP_C',x='DEPTH',data=MMMs[3])
scat.set_ylabel('MMM in degrees C')
scat.set_xlabel('Logger depth in meters')
scat.set_title('Depth vs MMM Hawaii')

from __future__ import absolute_import
import datetime
import os
import pandas as pd
import sys
import numpy as np
import matplotlib.pyplot as plt

#convert time_string to datetime.datetime
begin_date = datetime.datetime.strptime('2020-01-22', '%Y-%m-%d')
today = datetime.datetime.today()
days=int(str(today-begin_date).split(' ')[0])+1
days_list = [(begin_date+datetime.timedelta(days=i)).strftime('%m-%d-%Y') for i in range(0,days)]

kaggle_directory_path = 'https://raw.github.com/CSSEGISandData/COVID-19/master/csse_covid_19_data/csse_covid_19_daily_reports'
COVID19 = pd.DataFrame()
<<<<<<< HEAD
for i in days_list:
=======
for i in days_list[:-1]:
>>>>>>> b5f715ba7badfd370d76cc0dc0bdb32c81713705
    csv_file_path = os.path.join(kaggle_directory_path, i+'.csv')
    df = pd.read_csv(csv_file_path)
    COVID19 = COVID19.append(df, ignore_index=True)

# combine the duplicate columns into one
duplicate_columns = {'Last_Update': 'Last Update',
                     'Latitude': 'Lat',
                     'Longitude': 'Long_',
                     'Province_State': 'Province/State',
                     'Country_Region': 'Country/Region'
                    }
for i in duplicate_columns:
    COVID19[i] = COVID19[i].where(COVID19[i].notnull(), COVID19[duplicate_columns[i]])
    COVID19.drop(columns=duplicate_columns[i], inplace=True)

# standardize the format of Last_Update

def helper_month_day(s):
    return '0'+ s if len(s) == 1 else s

def helper_year(s):
    return '20'+ s if len(s) == 2 else s

def get_date(string):
    if 'T' in string:
        string = string.split('T')[0]
    else:
        string = string.split(' ')[0]
    if '/' in string:
        month, day, year = string.split('/')
        string = '-'.join([helper_year(year), helper_month_day(month), helper_month_day(day)])
    return string

COVID19['Last_Update'] = COVID19.apply(lambda row: get_date(row['Last_Update']), axis=1)

# check the format of Last_Update
#for index, value in COVID19.Last_Update.value_counts().sort_index().items():
#    print (index)

# check and find that Country_Region has no NaN records
#len(COVID19[COVID19.Country_Region.isnull()])

# for Province_State, replace null values(NaN) with 'NA'
# the reason to change null to 'NA' is to enable the sort_values().groupby().head(n=..),
# if np.nan exists, head() does not work out properly following groupby()
COVID19['Province_State'].fillna('NA', inplace=True)
# for Admin2, replace null values(NaN) with 'NA'
COVID19['Admin2'].fillna('NA', inplace=True)

# make sure in each day for each specific Country_Region & Province_State & Admin2(county) combination,
# there will be only one unique records
COVID19 = COVID19.sort_values(by=['Country_Region','Province_State','Admin2','Last_Update','Confirmed'],
                              ascending=False, na_position='last').groupby(
    by=['Country_Region','Province_State','Admin2','Last_Update']).head(1)


# get aggregated sum data by Country and Last_Update
def get_one_country_agg(country):
    agg_by_country_and_date = COVID19.groupby(by=['Country_Region','Last_Update']).agg(
                                   confirmed=pd.NamedAgg(column='Confirmed', aggfunc='sum'),
                                   death=pd.NamedAgg(column='Deaths',aggfunc='sum'),
                                   recovered=pd.NamedAgg(column='Recovered',aggfunc='sum')
                                   ).sort_values(by=['Country_Region','Last_Update'], axis=0)
    agg_by_country_and_date['daily_confirmed'] = agg_by_country_and_date.groupby(
                                                        by='Country_Region')['confirmed'].transform(
                                                        lambda x: x.diff())

    country = agg_by_country_and_date.loc[[country]].reset_index()
    return country

# get all US country data
us_all_level = COVID19[COVID19['Country_Region']=='US']
us_all_level.reset_index(inplace=True)
#us_all_level
#Check if a field is NaN
    # df.isnull().any(axis=0/1)
    # df.isnull().all(axis=0/1)


def date_filter(string, month_year_date): 
    if datetime.datetime.strptime(string, '%Y-%m-%d') >= datetime.datetime.strptime(
        '2020-{}'.format(month_year_date), '%Y-%m-%d'):
        return True
    return False

def get_data_since_specific_date(df, month_year_date):
    df['Last_update_after_specific_date'] = df.apply(lambda row: date_filter(row['Last_Update'], month_year_date), 
                                                     axis=1)
    return df[df['Last_update_after_specific_date'] == True].drop('Last_update_after_specific_date', axis=1)


# PREPARE THE DATA TO DO HEATMAPS
# remove rows with either missing Latitude or Longitude or both
us_all_level=us_all_level[us_all_level[['Latitude','Longitude']].notnull().all(axis=1)]
# remove rows with both Lat and Long equal to 0
us_all_level.drop(us_all_level[(us_all_level['Latitude'] == 0) & (us_all_level['Longitude'] == 0)].index, 
                  axis=0, inplace=True)


us_all_level.sort_values(by=['Latitude', 'Longitude', 'Last_Update'], inplace=True)
us_all_level['Daily_Confirmed'] = us_all_level.groupby(by=['Latitude', 'Longitude'])['Confirmed'].transform(
                                    lambda row: row.diff())

# remove the first row of each location group as the diff() makes their daily_confirmed to NaN
us_all_level = us_all_level[us_all_level['Daily_Confirmed'].notnull()]
# remove records with 0 daily_confirmed cases as we dont mark them in the map
us_all_level.drop(us_all_level[us_all_level['Daily_Confirmed'] == 0].index, axis=0, inplace=True)

import pandas as pd

import plotly.express as px

month_year_date = sys.argv[1]

df = get_data_since_specific_date(us_all_level, month_year_date).sort_values(by='Last_Update')
fig1 = px.density_mapbox(df, lat='Latitude', lon='Longitude', 
                        z='Daily_Confirmed', range_color=(0,1000), radius=15, 
                        opacity = 1,
                        center=dict(lat=38, lon=-95), zoom=2.8,
                        mapbox_style="stamen-terrain", animation_frame='Last_Update',
                        title='Heatmap of Daily Confirmed Cases')

fig2 = px.density_mapbox(df, lat='Latitude', lon='Longitude', 
                        z='Confirmed', range_color=(0,1000), radius=10, 
                        center=dict(lat=38, lon=-95), zoom=2.8,
                        mapbox_style="stamen-terrain", animation_frame='Last_Update',
                        title='Heatmap of Accumulated Confirmed Cases')
fig1.show()
fig2.show()



def create_color_scale_for_accum_confirmed(row):
    if 0 < row['Confirmed'] <= 50:
        return 0
    if 50 < row['Confirmed'] <= 100:
        return 1
    if 100 < row['Confirmed'] <= 300:
        return 2
    if 300 < row['Confirmed'] <= 500:
        return 3
    if 500 < row['Confirmed'] <= 750:
        return 4
    if 750 < row['Confirmed'] <= 1000:
        return 5
    if 1000 < row['Confirmed'] <= 2000:
        return 6
    if 2000 < row['Confirmed'] <= 3000:
        return 7
    if 3000 < row['Confirmed'] <= 4000:
        return 8
    if  4000< row['Confirmed'] <= 5000:
        return 9
    if 5000 < row['Confirmed'] <= 6000:
        return 10
    if 6000 < row['Confirmed'] <= 8000:
        return 11
    if 8000 < row['Confirmed'] <= 10000:
        return 12
    if 10000 < row['Confirmed'] <= 12500:
        return 13
    if 12500 < row['Confirmed'] <= 15000:
        return 14
    if 15000 < row['Confirmed'] <= 17500:
        return 15
    if 17500 < row['Confirmed'] <= 20000:
        return 16
    if 20000 < row['Confirmed'] <= 22500:
        return 17
    if 22500 < row['Confirmed'] <= 25000:
        return 18
    if row['Confirmed'] > 25000:
        return 19


def create_color_scale_for_daily_confirmed(row):
    if 0 < row['Daily_Confirmed'] <= 20:
        return '<20'
    if 20 < row['Daily_Confirmed'] <= 45:
        return '20-45'
    if 45 < row['Daily_Confirmed'] <= 70:
        return '45-70'
    if 70 < row['Daily_Confirmed'] <= 100:
        return '70-100'
    if 100< row['Daily_Confirmed'] <= 250:
        return '100-250'
    if 250 < row['Daily_Confirmed'] <= 500:
        return '250-500'
    if 500 < row['Daily_Confirmed'] <= 1000:
        return '500-1000'
    if 1000 < row['Daily_Confirmed'] <= 5000:
        return '1000-5000'
    if row['Daily_Confirmed'] > 5000:
        return '>5000'


def get_color_discrete_map2(df):
    
    daily_confirmed_color_scale = ['<20', '20-45', '45-70', '70-100','100-250', 
                                   '250-500', '500-1000', '1000-5000', '>5000']
    
    color_map={}
    for i in range(len(daily_confirmed_color_scale)):
        color_map[daily_confirmed_color_scale[i]]=css_color[i]   
    return color_map


def get_color_discrete_map1(df):
    css_color = ['#ffe6e6', '#ffcccc', '#ffb3b3', '#ff9999', '#ff8080', '#ff6666', '#ff4d4d',
            '#ff3333', '#ff1a1a', '#ff0000', '#e60000', '#cc0000', '#b30000', '#990000',
            '#8b0000', '#800000', '#660000', '#4d0000', '#330000', '#1a0000']
    color_map={}
    for i in range(len(df.Color_Scale_For_Accum_Confirmed.unique())):
        color_map[str(i)]=css_color[i]   
    return color_map


# scatter heatmap annimation of ACCUMULATED confirmed cases across US 
import pandas as pd
import plotly.express as px

# speicify a data from when to plot the heatmap, and order the data by date for annimation display in date order
df = get_data_since_specific_date(us_all_level, month_year_date).sort_values(by='Last_Update')
# create columns the value of which is used to assigned colors to the marks in the map
df['Color_Scale_For_Accum_Confirmed'] = df.apply(lambda row: create_color_scale_for_accum_confirmed(row), axis=1)
df['Color_Scale_For_Daily_Confirmed'] = df.apply(lambda row: create_color_scale_for_daily_confirmed(row), axis=1)

df['Size'] = 4

# remove rows where confirmed cases is 0, 
# as function create_color_scale() does not assign value nor mark on map for those 0 confirmed area.
df1=df[df['Color_Scale_For_Accum_Confirmed'].notnull()]

# narrow confirmed cases to make it more readable in map
df1['Narrowed_Confirmed'] = df1['Confirmed']*0.01
color_map = get_color_discrete_map1(df1)
fig1 = px.scatter_mapbox(df1, lat='Latitude', lon='Longitude', 
                        color='Color_Scale_For_Accum_Confirmed',
#                         color_discrete_map=color_map,
                        range_color=[0,19],
                        opacity=0.6,
                        size='Size',
                        size_max=4,
                        center=dict(lat=38, lon=-95), 
                        zoom=2.59,
                        mapbox_style="stamen-terrain", 
                        animation_frame='Last_Update',
                        title='Heatmap of Accumulated Confirmed Cases')


df2=df[df['Color_Scale_For_Daily_Confirmed'].notnull()]
df2['Narrowed_Confirmed'] = df2['Daily_Confirmed']*0.01
df2.sort_values(by=['Last_Update', 'Daily_Confirmed'], inplace=True)

css_color = ['#00008b', '#0000ff', '#00bfff', '#cc33ff',  
            '#cc0000', '#ff0000', '#ff8c00', '#ffd700', '#ffff00' ]
color_map = get_color_discrete_map2(df2)
fig2 = px.scatter_mapbox(df2, lat='Latitude', lon='Longitude', 
                        color='Color_Scale_For_Daily_Confirmed',
                        color_discrete_map=color_map,
                        opacity=0.8,
#                         size='Size',
                        size_max=4,
                        center=dict(lat=38, lon=-95), 
                        zoom=2.60,
                        hover_name='Combined_Key',
                        hover_data=['Last_Update','Daily_Confirmed'],
                        mapbox_style="stamen-terrain", 
                        animation_frame='Last_Update',
                        title='Heatmap of Daily Confirmed Cases')

fig2.update_layout(transition = {'duration': 0})
fig1.show()
fig2.show()

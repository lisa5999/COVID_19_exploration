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
for i in days_list:
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

# randomly pick date 2020-04-02 to verify that
# the number of records in US dataset is the same as that in the Kaggle .csv file
#print(len(us_all_level[us_all_level['Last_Update']=='2020-04-02']))
#us_all_level[us_all_level['Last_Update']=='2020-04-02']['Confirmed'].sum()


# get US agg data by state and date
def get_one_US_state_agg(us_state):
    agg_by_us_state_and_date = us_all_level.groupby(by=['Province_State','Last_Update']).agg(
                                                confirmed=pd.NamedAgg(column='Confirmed', aggfunc='sum'),
                                                death=pd.NamedAgg(column='Deaths',aggfunc='sum'),
                                                recovered=pd.NamedAgg(column='Recovered',aggfunc='sum')
                                                ).sort_values(by=['Province_State', 'Last_Update'])
    agg_by_us_state_and_date['daily_confirmed'] = agg_by_us_state_and_date.groupby(
                                                    by='Province_State')['confirmed'].transform(
                                                    lambda x: x.diff()) 
    return agg_by_us_state_and_date.loc[[us_state]].reset_index()

month_year_date = sys.argv[1]

# get daily confirmed cases for US, California, and Santa Clara county starting from 3/22
def date_filter(string, month_year_date):
    if datetime.datetime.strptime(string, '%Y-%m-%d') >= datetime.datetime.strptime(
        '2020-{}'.format(month_year_date), '%Y-%m-%d'):
        return True
    return False

def get_data_since_specific_date(df, month_year_date):
    df['Last_update_after_specific_date'] = df.apply(lambda row: date_filter(row['Last_Update'], month_year_date), axis=1)
    return df[df['Last_update_after_specific_date'] == True].drop('Last_update_after_specific_date', axis=1)


# define a function that auto plots daily confirmed cases by country and states
target_countries_or_states_map = {'US':'country', 
                                  'Italy':'country', 
                                  'United Kingdom':'country', 
                                  'France':'country', 
                                  'Germany':'country',
                                  'Canada':'country',
#                                   'India':'country',
                                  'Australia':'country',

                                  'California':'state', 
#                                   'New York':'state', 
#                                   'Massachusetts':'state',
                                  'Washington':'state',
#                                   'New Jersey':'state', 
                                  'Florida':'state',
#                                   'Michigan':'state', 
                                  'Pennsylvania':'state',
                                  'Illinois':'state',
                                  'Louisiana':'state',
                                  'Texas':'state'
                                  }
figure_color = ['b', 'g', 'r', 'y', 'm', 'k', 'c'] 

def plot_daily_confirmed_cases(target_countries_or_states_map):
    plt.figure(figsize=(20,12))
    i, j = 0, 0
    for target in target_countries_or_states_map:
        if target_countries_or_states_map[target] == 'country':
            agg_data = get_one_country_agg(target)
            agg_data_after_specific_date = get_data_since_specific_date(agg_data, month_year_date)
            dates = agg_data_after_specific_date['Last_Update']
            daily_confirmed = agg_data_after_specific_date['daily_confirmed']

            plt.subplot(211)
            plt.plot(dates, daily_confirmed, '{}--'.format(figure_color[i]), label=target)
            i += 1
        else:
            agg_data = get_one_US_state_agg(target)
            agg_data_after_specific_date = get_data_since_specific_date(agg_data, month_year_date)
            dates = agg_data_after_specific_date['Last_Update']
            daily_confirmed = agg_data_after_specific_date['daily_confirmed']

            plt.subplot(212)
            plt.plot(dates, daily_confirmed, '{}--'.format(figure_color[j]), label=target)
            j += 1
    plt.subplot(211)
    plt.xticks(rotation=60, fontsize=10)
    plt.legend()
    plt.title('Daily_Confirmed_by_Country')

    plt.subplot(212)
    plt.xticks(rotation=60, fontsize=10)
    plt.legend()
    plt.title('Daily_Confirmed_by_State')

    plt.tight_layout(pad=7, h_pad=3)	
    plt.suptitle('Daily Confirmed Cases Plotting')
    plt.show()
# plot daily confirmed cases by country and states
plot_daily_confirmed_cases(target_countries_or_states_map)

def US_and_states_in_one_plot(target_list):
    color = ['b', 'g', 'r', 'y', 'm', 'k', 'c'] 
    i=0
    plt.figure(figsize=(20,12))
    for target in target_list:
        if target =='US':
            data = get_one_country_agg(target)
        else:
            data = get_one_US_state_agg(target)
        data_after_specific_date = get_data_since_specific_date(data, month_year_date)
        dates = data_after_specific_date['Last_Update']
        daily_confirmed = data_after_specific_date['daily_confirmed']
       
        plt.plot(dates, daily_confirmed, '{}--'.format(color[i]), label=target)
        i += 1
    plt.xticks(rotation=60, fontsize=10)
    plt.legend()
    plt.title('Daily Confirmed Cases of US Hot Spots')

    plt.show()

target_list = ['US', 'New York', 'New Jersey', 'Massachusetts', 'Michigan']
US_and_states_in_one_plot(target_list)

#Useful ploting materails: 
#https://matplotlib.org/tutorials/introductory/pyplot.html


# get US agg data by state, county and date
def get_one_US_county_agg(us_state, us_county):
    agg_by_state_county_date = us_all_level.sort_values(by=['Province_State','Admin2','Last_Update'])
    agg_by_state_county_date['daily_confirmed'] = agg_by_state_county_date.groupby(
                                                    by=['Province_State','Admin2'])['Confirmed'].transform(
                                                    lambda x: x.diff())
    target = (agg_by_state_county_date.Province_State == us_state) & (agg_by_state_county_date.Admin2 == us_county)
    return agg_by_state_county_date[target].reset_index()

# santa_clara = get_one_US_county_agg('California', 'Santa Clara')
# santa_clara

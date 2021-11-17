
# -*- coding: utf-8 -*-
"""
Created on Mon Aug 23 12:43:02 2021

@author: dcoladne
"""

import requests
from datetime import datetime, timedelta
from datetime import date
import pandas as pd
import os
import math
import time

def build_url(lon, lat):
    #url = 'https://api.weather.gov/points/{0},{1}/forecast'.format(lat, lon)
    url = 'https://api.weather.gov/points/{0},{1}'.format(lat, lon)
    r = requests.get(url)
    #print(r.json()['properties'])
    #forecast_url = r.json()['properties']['forecast']
    forecast_url = r.json()['properties']['forecastHourly']
    return forecast_url

# url = build_url(-74.3, 40.6)  # ny
url = build_url(-116.3, 43.6)  # boi
r = requests.get(url)

r_json = r.json()

#print([[r_json['properties']['periods'][i]['temperature'], 
#        r_json['properties']['periods'][i]['name'],
#        r_json['properties']['periods'][i]['shortForecast']]
#       for i in range(len(r_json['properties']['periods']))])

temps_df = pd.DataFrame(columns=['day', 'avg'])

for i in range(len(r_json['properties']['periods'])):
    cd_string = r_json['properties']['periods'][i]['startTime']
    temp = r_json['properties']['periods'][i]['temperature']
    temps_df = temps_df.append({'day':cd_string, 'avg':temp}, ignore_index=True)

temps_df.index.name = 'index'
avg_temps_df = temps_df.groupby(['day']).mean()

print(avg_temps_df)




#for i in range(len(r_json['properties']['periods'])):
#    print([r_json['properties']['periods'][i]['temperature'], 
#            r_json['properties']['periods'][i]['name'],
#            r_json['properties']['periods'][i]['startTime'],
#            r_json['properties']['periods'][i]['shortForecast']])



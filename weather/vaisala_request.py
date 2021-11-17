
"""
Pulls 8 stations closest to a lat/lon point, then queries VAISALA using those station codes and the date range. 
Finally we use inverse square with temperature and distance from the point to get an estimation at that point. 
"""
import pyodbc
import pandas as pd
import pprint as pp
import os
import config
import requests
import xml.etree.ElementTree as et
from datetime import datetime, timedelta
from datetime import date

__authors__ = "Jordan Hiatt"

class VaisalaObject():
    # The worker parameter is in case this object is used with a GUI, which would emit signals that updates the GUI from a separate thread. 
    def __init__(self, lat, lon, worker=None):
        self.worker = worker
        self.lat = lat
        self.lon = lon

    def cel_to_faren(self, temp):
        return temp*(9/5)+32.0

    # Pulls all of the closest stations along with their distances to the point. 
    def get_station_ids(self):
        conn = pyodbc.connect('Driver={SQL Server};'
                            'Server=ITD9HTSPC219328;'
                            'Database=wx_history;'
                            'Trusted_Connection=yes;')
        # print('{} {}'.format(self.lat, self.lon))
        cursor = conn.cursor()
        df = pd.DataFrame(columns=['name', 'id', 'lat', 'lon', 'distance'])
        cursor.execute("""
        DECLARE @Latitude FLOAT SET @Latitude   =   {}
        DECLARE @Longitude FLOAT SET @Longitude = {}
        DECLARE @DOI Date SET @DOI              = '2021-04-05'
        DECLARE @HOWMANYSITETOFIGURE INTEGER SET @HOWMANYSITETOFIGURE = 8
        DECLARE @RADIUSTOSITES FLOAT SET @RADIUSTOSITES = 50 -- in miles
        DECLARE @IDW_POWER FLOAT SET @IDW_POWER = 1.2

        SELECT  -- (would be more efficient to compute distance in a further out query, but I got this to work!)
            TOP 8 Station_Name, Station_ID, its.Lat__Decimal, its.Long__Decimal
            ,( 3959 * acos( cos( radians(@Latitude) ) * cos( radians( its.Lat__Decimal ) ) * cos( radians(its.Long__Decimal) - radians(@Longitude) ) + sin( radians(@Latitude) ) * sin( radians(its.Lat__Decimal)))) AS distance_to_site 
        FROM RWIS_Station_Locations as its
        Order by distance_to_site
        """.format(self.lat, self.lon))
        for row in cursor:
            df = df.append({'name': row[0], 'id': row[1], 'lat':row[2], 'lon':row[3], 'distance':row[4]}, ignore_index=True)
        return(df)

    # Query the vaisala database over a range of dates. It gives back xml which needs to be parsed. 
    # To get the full day we need to push the request ahead by a day and drop the last entry in the dataframe
    def get_vaisala_xml(self, siteid, fromdate, todate):
        #date format is YYYY-mm-dd[ HH:MM:SS]  (todate is non-inclusive)
        to_date_forward = datetime.strptime(todate, '%Y-%m-%d')+timedelta(hours=24)
        to_date_forward_str = to_date_forward.strftime("%Y-%m-%d")
        url = 'https://exportdb.vaisala.io/export?username=idt&password=Data4Idaho'
        if len(str(siteid)) > 0:
            url = url + '&station='+str(siteid)
        if len(fromdate) > 0 and len(todate) > 0:
            url = url + '&earliesttime=' + fromdate + '&latesttime='+ to_date_forward_str 
        r = requests.get(url)
        xml_string = r.text
        site = []
        timestamp = []
        t    = []
        rh   = []
        ts   = []
        st   = []
        try:
            xroot = et.fromstring(xml_string)
        except et.ParseError:
            return pd.DataFrame({'site':site, 'timestamp':timestamp, 't':t, 'rh':rh, 'ts':ts, 'st':st})
        for instance in xroot:
            for name in instance.iter('name'):
                foo1 = name.text
            for resultOf in instance.iter('resultOf'):
                foo2 = resultOf.get('timestamp')
                foo3, foo4, foo5, foo6 = -999.99,-999.99,-999.99,-999.99,
                for val in resultOf.findall('value'):
                    if val.get('code') == 'T':
                        foo3 = float(val.text)
                    if val.get('code') == 'RH':
                        foo4 = float(val.text)
                    if val.get('code') == 'TS':
                        foo5 = float(val.text)
                    if val.get('code') == 'ST':
                        foo6 = float(val.text)
                site.append(foo1)
                timestamp.append(foo2)
                t.append(foo3)
                rh.append(foo4)
                ts.append(foo5)
                st.append(foo6)
        df = pd.DataFrame({'site':site, 'timestamp':timestamp, 't':t, 'rh':rh, 'ts':ts, 'st':st})
        df['day'] = df['timestamp'].str.slice(0, 10)
        return df[:-1]

    def get_hi_lo_interpolated(self, start_date, end_date):
        result_df = pd.DataFrame()
        df_list = []
        date_temp_tuples = []
        stations_df = self.get_station_ids()
        for i, row in stations_df.iterrows():
            # Emitted signal in the case of a PyQt GUI using the object from a worker 
            if self.worker is not None:
                self.worker.emit_progress("Getting data from VAISALA station {} out of {}\n".format(i+1, len(stations_df)))
                if config.cancel_flag:
                    raise Exception('Operation Cancelled')

            # get_vaisala_xml could possibly return an empty dataframe if the station isn't working
            vai_df = self.get_vaisala_xml(row[1],start_date, end_date)
            if not vai_df.empty:
                vai_df['distance'] = row[4]
                grouped_df = pd.DataFrame()
                grouped_df['hi'] = vai_df.groupby(by="day")['t'].max()
                grouped_df['lo'] = vai_df.groupby(by="day")['t'].min()
                grouped_df['distance'] = row[4]
                df_list.append(grouped_df)
        date_range = pd.date_range(start=start_date, end=end_date)
        for date in date_range: 
            hi_num, lo_num, den_sum = 0,0,0
            date_string = date.strftime('%Y-%m-%d')
            for df in df_list:
                dist = df['distance'].iloc[0]
                hi = df['hi'].loc[date_string]
                lo = df['lo'].loc[date_string]
                hi_num += hi/(dist**1.2)
                lo_num += lo/(dist**1.2)
                den_sum += 1/(dist**1.2)
            interp_hi = (hi_num/den_sum)
            interp_lo = (lo_num/den_sum)            
            result_df = result_df.append({'day': date_string,'high': self.cel_to_faren(interp_hi), 'low': self.cel_to_faren(interp_lo)}, ignore_index=True)
            print(result_df)
        return result_df

    # TODO: existing file must match metadata of request to bother appending. 
    # Looks for historical_data created by the SQL query, and creates a new .csv by appending this data onto that file. 
    def append_existing_file(self, in_df):
        # if file exists, check most recent date 
        today_string = date.today().strftime('%Y-%m-%d')
        # file_name = 'historical_data.csv'
        # append_file_name = 'CsvFiles\\appended_historical_data.csv'
        # if os.path.exists(file_name):
            # reading csv have to set index col to 1 to grab the 'day' from it
            # df = pd.read_csv('historical_data.csv', header=0)
        df = in_df.copy()
        last_day_string = df['day'].iloc[-1]
        next_day_after_last = (datetime.strptime(last_day_string, '%Y-%m-%d')+timedelta(days=1)).strftime('%Y-%m-%d')
        df = df.append(self.get_hi_lo_interpolated(next_day_after_last, today_string), ignore_index=True)

        # create a new file as to not ruin the one that takes forever to make
        # if os.path.exists(append_file_name):
        #     os.remove(append_file_name)
        # df.to_csv(append_file_name, index=False)
        return df
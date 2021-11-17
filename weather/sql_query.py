
"""
Using David's SQL query, pulls the high/low temperatures every day over a range of dates. 
If there are questions about how the query works, ask David. 
Creates a .csv which is the starting point before filling in vaisala data and a 7-day forecast. 
"""
import pyodbc
import pandas as pd
import pprint as pp
import os
import config

__authors__ = ["Jordan Hiatt", "David Coladner"]

# ITD9HTSPC219328
def pull_data(start_date, end_date, lat, lon):
    # Setting the timeout tells the user if they're connected to the database
    conn = pyodbc.connect('Driver={SQL Server};'
                        'Server=ITD9HTSPC219328;'
                        'Database=wx_history;'
                        'Trusted_Connection=yes;', timeout=1)
    cursor = conn.cursor()
    hi_lo_list = []
    df = pd.DataFrame(columns=['day', 'high', 'low'])
    cursor.execute("""
    DECLARE @Latitude FLOAT SET @Latitude   =   {}
    DECLARE @Longitude FLOAT SET @Longitude = {}
    DECLARE @START_DATE DATE SET @START_DATE = '{}'
    DECLARE @END_DATE DATE SET @END_DATE = '{}'
    DECLARE @HOWMANYSITETOFIGURE INTEGER SET @HOWMANYSITETOFIGURE = 8
    DECLARE @RADIUSTOSITES FLOAT SET @RADIUSTOSITES = 50 -- in miles
    DECLARE @IDW_POWER FLOAT SET @IDW_POWER = 1.2
    SELECT vds.dt_iso, sum(hi/POWER(distance_to_site,@IDW_POWER))/sum(POWER(1/distance_to_site,@IDW_POWER)) as hi_idw, 
    sum(lo/POWER(distance_to_site,@IDW_POWER))/sum(POWER(1/distance_to_site,@IDW_POWER)) as lo_idw 
    FROM (
        SELECT
        site, dt_iso, max(Air_Temp) as hi, min(Air_Temp) as lo,
        avg(Lat__Decimal) as lat, avg(Long__Decimal) as lon, avg(distance_to_site) as distance_to_site 
            FROM (
            SELECT -- (would be more efficient to compute distance in a further out query, but I got this to work!)
                concat(substring(dt,7,5),'-',substring(dt,4,2),'-',substring(dt,1,2)) as dt_iso
                ,substring(Timestamp,12,5) as tod, [Timestamp],[Surf_Temp], [Surf_State], [Air_Temp], [Rain_State], rwis.[site], [dt]
                ,its.Lat__Decimal, its.Long__Decimal
                ,( 3959 * acos( cos( radians(@Latitude) ) * cos( radians( its.Lat__Decimal ) ) * cos( radians(its.Long__Decimal) - radians(@Longitude) ) + sin( radians(@Latitude) ) * sin( radians(its.Lat__Decimal)))) AS distance_to_site 
            FROM [wx_history].[dbo].[rwis_view] as rwis
            LEFT JOIN RWIS_Station_Locations as its
                ON rwis.site=its.Station_Name ) as r
            where r.dt_iso BETWEEN @START_DATE AND @END_DATE
            and Air_Temp is not null 
            and distance_to_site is not null and distance_to_site < @RADIUSTOSITES 
            group by dt_iso, r.site) as vds
            group by vds.dt_iso
            ORDER BY vds.dt_iso
    """.format(lat, lon, start_date, end_date))

    for row in cursor:
        df = df.append({'day':row[0], 'high':row[1], 'low':row[2]},ignore_index=True)

    # Drop na values and where the high is equal to the low, because that day can't be used
    df = df.dropna()
    df = df[df.high != df.low]

    # file_name = 'historical_data.csv'
    # if os.path.exists(file_name):
    #     os.remove(file_name)
    # df.to_csv(file_name, index=False)
    return df
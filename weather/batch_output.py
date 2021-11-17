
"""
Input a .csv with format "RouteID, Measure, Lat, Lon" and a spreadsheet will be created for every point. 
"""
import sys, traceback, os, glob
import pandas as pd
from datetime import date, time, timedelta
from PyQt5.QtWidgets import QApplication, QWidget, QInputDialog, QLineEdit, QFileDialog
from PyQt5.QtGui import QIcon
import sql_query, vaisala_request, raster_operations, emulate_spreadsheet
import dataframe_image as dfi

__authors__ = "Jordan Hiatt"

def create_folder_path(folder_name):
    folder_path = os.getcwd()+'\\'+folder_name
    if not os.path.isdir(folder_path):
        os.makedirs(folder_path)
    return folder_path

def create_file_path(lat, lon, folder_path):
    today_string = date.today().strftime('%Y-%m-%d')
    coord_string = '%.3f_%.3f' % (lat, lon)
    file_name = '{}_{}.csv'.format(coord_string, today_string)
    file_path = folder_path+'\\'+file_name
    return file_path

# TODO: Every process should just return a DF instead of creating .csv files
# So pull_data returns a DF, then append_existing_file accepts a df and returns one
def batch_run(mp_coord_df, point_type):
    """Long-running task."""
    #prev_year = (date.today()+timedelta(days=-365)).year 
    prev_year = (date.today()+timedelta(days=-270)).year  # just get us back to year of prev Oct 1. (Julian Date of Oct 1 is normally 274)
    #prev_year = 2021
    prev_year_start = '{}-10-01'.format(prev_year)
    today_string = date.today().strftime('%Y-%m-%d')
    closure_date_df = pd.DataFrame()
    files_downloaded = False

    for i, row in enumerate(mp_coord_df.iterrows()):
        try: 
            # TODO: Allow routes or segcodes
            if point_type == 'RouteNo':
                # TODO: Where is segcode? 
                lat = row[1]['LAT']
                lon = row[1]['LON']
                route_code = row[1]['ROUTE']
                location = row[1]['MILEPOINTER']
                id = row[1]['id']
                #mile_range = row[1]['RANGE']
                measure = 'MilePoint'
            else:
                lat = row[1]['lat']
                lon = row[1]['lon']
                measure = 'Measure'
                location = row[1]['Measure']

            point_string = "{}: {}\t {}: {}".format(point_type, row[1][0], measure, location)

            # RWIS
            print('Pulling historical RWIS data at point ({}, {}) from {} to {}...\n'.format(lat, lon, prev_year_start, today_string))
            df = sql_query.pull_data(prev_year_start, today_string, lat, lon)

            # VAISALA
            print('Pulling very recent RWIS data from Vaisala API at point ({}, {}) from {} to {}...\n'.format(lat, lon, prev_year_start, today_string))
            vaisala = vaisala_request.VaisalaObject(lat, lon)
            df = vaisala.append_existing_file(df)

            # NOAA
            print('Pulling 7 day forecast data from NOAA (this may take a while)...\n')
            raster = raster_operations.Raster(lat, lon, files_downloaded)
            files_downloaded = True
            df = raster.get_avg_at_coordinate(df)

            # Spreadsheet
            df = emulate_spreadsheet.build_emulated_spreadsheet(lat, lon, point_string, df)

            # Create folder in current directory and save df as .csv 
            folder_path = create_folder_path("Spreadsheets")
            file_path = create_file_path(lat, lon, folder_path)
            if os.path.exists(file_path):
                os.remove(file_path)
            df.to_csv(file_path, index=False)
            # html_file = df[-7:].to_html()

            if point_type == 'RouteNo':
                closure_date = df[df['message'] == "CUMULATIVE THAWING INDEX > 25: IMPOSE BREAKUP LIMITS"].copy() # we get the day and average from here
                closure_date['ROUTE'] = route_code
                closure_date['MILEPOINTER'] = location
                #closure_date['RANGE'] = mile_range
                closure_date['id'] = id
                closure_date['LAT'] = lat
                closure_date['LON'] = lon
                closure_date_df = closure_date_df.append(closure_date, ignore_index=True)

                brokenbeforelist = df[df['message'] == "CUMULATIVE THAWING INDEX > 25: IMPOSE BREAKUP LIMITS"].index.tolist()
                if len(brokenbeforelist) == 0:
                    print('Not broken up yet.  Listing last 7 days')
                    mini_df = df[-7:].copy()
                else:
                    closure_index = df[df['message'] == "CUMULATIVE THAWING INDEX > 25: IMPOSE BREAKUP LIMITS"].index.tolist()[0]
                    mini_df = df[closure_index:closure_index+7].copy()
                # So this gets the last 7 days, but what would be better is the location of the closure date right? 
                # mini_df = df[-7:].copy()
                # mini_df = df[closure_index:closure_index+7].copy()
                df_styled = mini_df.style.background_gradient()

                # This overwrites .png with the same name, so if there are multiple items with id '1', then there will only be one '1.png' in the end. 
                # The point is that the milepost range should have one unified answer, corresponding to whichever output has the earliest spring breakup. 
                # All points along the range will reference the same image. 
                dfi.export(df_styled,"SpreadSheets\\{}.png".format(id))
                # dfi.export(df_styled,r'\\itdexpwsp01\Apps\dataanalytics\SpringBreakupReports'+"\\{}.png".format(id))

        except Exception as e:
            traceback.print_exc()
            print(str(e))
    closure_date_df['average'] = closure_date_df['average'].round(2)
    file_name = 'closure_dates.csv'
    if os.path.exists(file_name):
        os.remove(file_name)

    # Get the id of the min of each group, and reduce the df to only those rows. 
    # Must be turned into datetime to get a min value
    closure_date_df['day'] = pd.to_datetime(closure_date_df['day']) 
    closure_date_df = closure_date_df.iloc[closure_date_df.groupby(['id'])['day'].idxmin(axis=0)]

    # closure_date_df = closure_date_df[['id', 'day', 'average', 'ROUTE', 'RANGE', 'MILEPOINTER', 'LAT', 'LON']]
    #closure_date_df[['id', 'day', 'average', 'ROUTE', 'RANGE', 'MILEPOINTER', 'LAT', 'LON']].to_csv(file_name, index=False)
    closure_date_df[['id', 'day', 'average', 'ROUTE', 'MILEPOINTER', 'LAT', 'LON']].to_csv(file_name, index=False)
    # closure_date_df[['min_closure', 'day', 'average', 'ROUTE', 'MILEPOINTER', 'LAT', 'LON', 'CODE']].groupby(by=['CODE'])['day'].min().to_csv(file_name, index=False)
        
def main(mp_coord_df, arg):
    # take in excel spreadsheet of mile pointers, convert to coordinates
    # run each thing and save in a spreadsheets folder
    # TODO: CSV should have type of input and measure at the beginning of the file
    if arg == '-s':
        batch_run(mp_coord_df, 'SegCode')
    elif arg == '-r':
        batch_run(mp_coord_df, 'RouteNo')
    else:
        print('Argument error, use -s for SegCode/MP and -r for RouteNo/MP')



if __name__ == "__main__":
    if len(sys.argv) == 3:
        # Remove all current files
        files = glob.glob('Spreadsheets'+'\*')
        for f in files:
            os.remove(f)
            
        # TODO: Uncomment this to do a permission test on the ITD folder. 
        # leaf_dir = r'permission_test'
        # parent_dir = r'\\itdexpwsp01\Apps\dataanalytics\SpringBreakupReports'
        # path = os.path.join(parent_dir, leaf_dir)
        # try:
        #     os.makedirs(path)
        #     os.rmdir(path)
        # except:
        #     print("You don't have permission to create files in \\itdexpwsp01\Apps\dataanalytics\SpringBreakupReports")
        #     exit()
        # TODO: Clear out SpringBreakupReports if user has permission


        # If this is the primary file and not used as a module, check arguments for .csv
        df = pd.read_csv(sys.argv[1])
        # TODO Remember that this is cutting off the .csv
        #df = df[:3]
        main(df, sys.argv[2])
    else:
        print("Error: python batch_outut.py <filename.csv> <-s/-r>")
        # output_routeno_milepointer
        print("Try routes_with_lon_lat.csv or ROUTENO_MILEPOINTER.csv")
        print("Use -s for SegCode/MP and -r for RouteNo/MP")
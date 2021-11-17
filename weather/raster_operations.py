
"""
Downloads the GRIB files from NOAA, converts lat/lon point to a point on the grid, and outputs a csv with the high/low temp for each day. 
The elements of this .csv are meant to be tacked onto the bigger one that has temperature data going back to October of the previous year. 
"""
__authors__ = "Jordan Hiatt"

import os
import math
import time
from datetime import datetime, timedelta
from datetime import date
import requests
import pandas as pd
import config
# proj.db lets us create projections, there's a problem where osgeo can't find the proj or gdal stuff
current_dir = os.getcwd()
os.environ['PROJ_LIB'] = current_dir+'\\share\\proj'
os.environ['GDAL_DATA'] = current_dir+'\\share'
from osgeo import gdal

file_paths = []
file_paths.append(('days1to3min.bin','min','https://tgftp.nws.noaa.gov/SL.us008001/ST.opnl/DF.gr2/DC.ndfd/AR.pacnwest/VP.001-003/ds.mint.bin'))
file_paths.append(('days4to7min.bin','min', 'https://tgftp.nws.noaa.gov/SL.us008001/ST.opnl/DF.gr2/DC.ndfd/AR.pacnwest/VP.004-007/ds.mint.bin'))
file_paths.append(('days1to3max.bin','max', 'https://tgftp.nws.noaa.gov/SL.us008001/ST.opnl/DF.gr2/DC.ndfd/AR.pacnwest/VP.001-003/ds.maxt.bin'))
file_paths.append(('days4to7max.bin','max', 'https://tgftp.nws.noaa.gov/SL.us008001/ST.opnl/DF.gr2/DC.ndfd/AR.pacnwest/VP.004-007/ds.maxt.bin'))
# avg_temps_filename = 'forecast_temps.csv'

class Raster():
    def __init__(self, pos_north, pos_west, files_downloaded, worker=None):
        # global file_paths
        self.pos_north = pos_north
        self.pos_west = pos_west
        self.worker = worker
        self.files_downloaded = files_downloaded
        
    # Downloads the files with hardcoded links. Usually fails on the first couple tries, but after the first success doesn't fail again. 
    def download_with_retry(self, f):
        remaining_download_tries = 15

        while remaining_download_tries > 0 :
            if config.cancel_flag:
                raise Exception('Operation Cancelled')
            try:
                response = requests.get(f[2], verify=False, timeout=10)
                open(f[0], 'wb').write(response.content)
                print('{} download complete'.format(f[0]))
                if self.worker is not None:
                    self.worker.emit_progress('{} download complete\n'.format(f[0]))
            except Exception as e:
                print("Error downloading " + f[0] +" on trial: " + str(16 - remaining_download_tries))
                if self.worker is not None:
                    self.worker.emit_progress("Error downloading {} on trial: {}\n".format(f[0], str(16 - remaining_download_tries)))
                remaining_download_tries = remaining_download_tries - 1
                time.sleep(2)
                continue
            else:
                break

    def download_files(self):
        for f in file_paths:
            self.download_with_retry(f)

    @staticmethod
    def clear_files(file_paths):
        for f in file_paths:
            if os.path.exists(f[0]):
                os.remove(f[0])

    def get_avg_at_coordinate(self, in_df):
        # Clear files before and after function call 
        # self.clear_files(file_paths)
        # self.clear_files([avg_temps_filename])

        # grib needs coordinates positive
        self.pos_west = -self.pos_west
        print(self.files_downloaded)
        # TODO: if files current, don't download and don't clear files. Clear the files from batch_output
        if not self.files_downloaded:
            # Before downloading any files we clear everything out
            self.clear_files(file_paths)
            print('Temporary files cleared')        
            self.download_files()

        min_list = []
        max_list = [] 

        date_checked = False

        for grb_file in file_paths:
            # Open the grib file
            input_raster = gdal.Open(grb_file[0])
            output_raster = current_dir+"\\temp.grb"

            # Change lambert projection to epsg4326
            warp_object = gdal.WarpOptions(dstSRS='EPSG:4326')
            warp = gdal.Warp(output_raster, input_raster, dstSRS='EPSG:4326')
            # Close the files
            warp = None 

            # Open the new grib file created
            data = gdal.Open(output_raster)
            geo_transform = data.GetGeoTransform()
            x_pixel_size = geo_transform[1]

            # The y pixel size is negative, so we make it positive here
            y_pixel_size = -geo_transform[5]

            # print('geo_transform: {}'.format(geo_transform))
            # print('raster x size: {} raster y size: {}'.format(data.RasterXSize, data.RasterYSize))
            minx = geo_transform[0]

            # The minx + (size of grid point * amount of points in grid) is the max x coordinate
            maxx = minx + geo_transform[1] * data.RasterXSize
            maxy = geo_transform[3]
            miny = maxy + geo_transform[5] * data.RasterYSize

            # Change negative east to positive west
            positive_minx = -minx

            # The top left of the grid array at (0,0) is the max latitude and min longitude
            # We need to get the point's coordinate distance from the origin to get the position in the grid. 
            x_distance_from_origin = abs(positive_minx-self.pos_west) 
            y_distance_from_origin = abs(maxy-self.pos_north)

            # Dividing coordinate distance by size of each pixel gets us the number of grid points from the origin.
            # Now we have the location on the grid and can pull it from the array.
            row = math.floor(x_distance_from_origin/x_pixel_size)
            col = math.floor(y_distance_from_origin/y_pixel_size)

            raster_count = data.RasterCount
            for i in range(1, raster_count + 1):
                # Get the date of the current band
                band = data.GetRasterBand(i)
                epoch_time = int(band.GetMetadata().get('GRIB_VALID_TIME').split()[0])
                band_date = time.strftime('%m-%d', time.localtime(epoch_time))
                # only check the current date once
                if not date_checked:
                    date_checked = True
                    todays_date = date.today().strftime("%m-%d")
                    tomorrows_date = (date.today()+timedelta(hours=24)).strftime("%m-%d")
                    if(band_date == tomorrows_date):
                        print('Current date matches grib file')
                    else:
                        print('Gribfile out of date')
                        print('Band {} is {} and tomorrow is {}'.format(i, band_date, tomorrows_date))

                data_array = band.ReadAsArray()
                # The raster band is read as Y, X or lat/lon. 
                celsius = data_array[col, row]
                faren = (celsius * 9/5) + 32 

                # Skip today's forecast
                if not (band_date == todays_date) and (grb_file[1] == 'max'):
                    max_list.append(faren)
                else:
                    min_list.append(faren)

            data = None
            input_raster = None
            if os.path.exists(output_raster):
                os.remove(output_raster)

        # Zip the two list and get the avg temp for each day
        avg_temps = []
        avg_temps_df = pd.DataFrame(columns=['day', 'low', 'high'])
        current_date = date.today()
        current_date_string = current_date.strftime("%Y-%m-%d")

        for num1, num2 in zip(min_list, max_list):
            current_date = current_date+timedelta(hours=24)
            cd_string = current_date.strftime("%Y-%m-%d")
            avg = (num1+num2)/2
            tup = (avg, cd_string)
            avg_temps.append(tup)
            avg_temps_df = avg_temps_df.append({'day':cd_string, 'low': num1, 'high': num2}, ignore_index=True)

        avg_temps_df.index.name = 'index'

        # Create avg_temps csv to be appended onto the historical data
        # print('Writing forecast_temps.csv')
        # self.clear_files([avg_temps_filename])
        # df = pd.read_csv('CsvFiles\\appended_historical_data.csv', header=0)
        # df2 = pd.read_csv('forecast_temps.csv', header=0)
        # df = df.append(df2, ignore_index=True)

        #TODO: do we need to do a .copy()?
        in_df = in_df.append(avg_temps_df, ignore_index=True)

        # avg_temps_df.to_csv(avg_temps_filename)

        return in_df

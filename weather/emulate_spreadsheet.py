# use historical_data.csv and the formulas in the spreadsheet to emulate results. 
# use an existing spreadsheet and plug in to see if freeze and thaw days are the same
"""
A neighboring state uses a spreadsheet that utilizes freezing and thawing indices over time to determine if load limits have to be applied. 
This takes a list of daily average temperatures and performs the same operations as the spreadsheet but in a dataframe and outputs a .csv. 
"""
from geopy import location
import pandas as pd
from decimal import Decimal, ROUND_UP
import pprint
import os
import pathlib
from datetime import timedelta, date, datetime
import config
pathlib.Path(__file__).parent.absolute()

__authors__ = "Jordan Hiatt"

# Everything must be rounded to one decimal place
def round_up_tenth(float_input):
    return float(Decimal(str(float_input)).quantize(Decimal('.1'), rounding=ROUND_UP))

# TODO: this function reads in two dataframes, I want the dataframe to be prepared before the function is called. 
def build_emulated_spreadsheet(lat, lon, point_location, df):
    current_year = str(date.today().year)
    output_str = ''
    # TEST DATA
    # df = pd.read_csv('test_hi_lo.csv', names=['high', 'low'])
    # df['average'] = (df['high']+df['low'])/2.0
    # df['day'] = pd.date_range(start='10/1/2020', periods=len(df))

    # NORMAL RUN 
    # TODO: Figure out how to pull from the folder
    # df = pd.read_csv('CsvFiles\\appended_historical_data.csv', header=0)
    # df2 = pd.read_csv('forecast_temps.csv', header=0)
    # df = df.append(df2, ignore_index=True)
    df['average'] = (df['high']+df['low'])/2.0

    df['32-avg/2'] = ((32.0-df['average'])/2.0)
    ref_temp = pd.read_csv('reference_temp.csv', header=None)
    df['ref_temp'] = ref_temp

    #if average - ref < 0, make 0, else average - ref
    df['daily_thawing_index'] = (df['average'] - df['ref_temp']).apply(lambda x: 0.0 if x < 0.0 else x)

    # =IF(AND(L5=0,P4>M5),32-J5,0)
    df['daily_freezing_index'] = 0.0
    df['cumulative_freezing_index'] = 0.0
    # set the first CTI entry to the first DTI entry
    df['cumulative_thawing_index'] = 0.0
    df.at[0, 'cumulative_thawing_index'] = df['daily_thawing_index'].iloc[0]
    df['roadway_status'] = ''
    df['message'] = ''

    # flags
    freezing_started = False
    winter_load_increased = False
    cti_reset = False
    cfi_reset = False
    thawing_started = False
    breakup_limits_started = False
    eight_weeks_since_breakup_limits = False
    normal_limits_restarted = False
    overweight_started = False

    # Start at 1 since we'll be using the previous index
    for i in range(1, len(df)):
        date_string = df.loc[i, 'day']
        dfi = 0
        dti = df.loc[i, 'daily_thawing_index']
        diff = df.loc[i, '32-avg/2']
        avg = df.loc[i, 'average']
        prev_cti = df.loc[i-1, 'cumulative_thawing_index'] 
        prev_cfi = df.loc[i-1, 'cumulative_freezing_index']
        if dti == 0 and prev_cti > diff:
            dfi = 32.0-avg
        else:
            dfi = 0
        df.at[i, 'daily_freezing_index'] = dfi
        if date_string == pd.to_datetime('7/1/{}'.format(current_year)).strftime("%Y-%m-%d"): 
            print('CFI RESET ON: {}\n'.format(date_string))
            output_str += 'CFI RESET ON: {}\n'.format(date_string)
            cfi_reset = True
            current_cfi = 0
        else:
            current_cfi = prev_cfi + dfi
        df.at[i, 'cumulative_freezing_index'] = current_cfi

        # if prev cti + current dti - current dfi/2 > 0, use that else 0
        if date_string == pd.to_datetime('1/1/{}'.format(current_year)).strftime("%Y-%m-%d") and not cti_reset: #current year
            current_cti = 0
            cti_reset = True
            print('{} CTI RESET\n'.format(date_string))
            output_str += '{} CTI RESET\n'.format(date_string)
        else:
            current_cti = prev_cti + dti - dfi/2.0

        if current_cti > 0:
            df.at[i, 'cumulative_thawing_index'] = current_cti
        else:
            df.at[i, 'cumulative_thawing_index'] = 0

        #L4 is dti, N4 is dfi
        # =IF(AND(L4=0,N4=0),"NO THAW",IF(AND(L4>0,N4=0),"THAWING","REFREEZING"))
        if dti == 0 and dfi == 0:
            df.at[i, 'roadway_status'] = 'NO THAW'
        else:
            if dti > 0 and dfi == 0:
                df.at[i, 'roadway_status'] = 'THAWING'
            else:
                df.at[i, 'roadway_status'] = 'REFREEZING'

        #flag checks, this comes straight off the spreadsheet
        if not freezing_started and dfi > 0.0:
            print('{} FREEZING STARTED\n'.format(date_string))
            output_str += '{} FREEZING STARTED\n'.format(date_string)
            freezing_started = True
            df.at[i, 'message'] = 'FREEZING STARTED'

        if not winter_load_increased and current_cfi > 280.0:
            print('{} WINTER LOAD INCREASED\n'.format(date_string))
            output_str += '{} WINTER LOAD INCREASED\n'.format(date_string)
            winter_load_increased = True
            df.at[i, 'message'] = 'WINTER LOAD INCREASED'

        if not thawing_started and cti_reset and current_cti > 0.0:
            print('{} THAWING BEGINS: RESCIND WINTER LOAD INCREASES\n'.format(date_string))
            output_str += '{} THAWING BEGINS: RESCIND WINTER LOAD INCREASES\n'.format(date_string)
            thawing_started = True
            df.at[i, 'message'] = 'THAWING BEGINS: RESCIND WINTER LOAD INCREASES'

        if not breakup_limits_started and thawing_started and current_cti > 25.0:
            print('{} CUMULATIVE THAWING INDEX > 25: IMPOSE BREAKUP LIMITS\n'.format(date_string))
            output_str += '{} CUMULATIVE THAWING INDEX > 25: IMPOSE BREAKUP LIMITS\n'.format(date_string)
            breakup_limits_started = True
            breakup_end_date = pd.date_range(start=date_string, periods=56)[-1].strftime("%Y-%m-%d") # final day of 8 week range from present
            df.at[i, 'message'] = 'CUMULATIVE THAWING INDEX > 25: IMPOSE BREAKUP LIMITS'
        
        if not normal_limits_restarted and breakup_limits_started and current_cti > 25.0 and date_string == breakup_end_date:
            print('{} CTI > 25 + 8-WEEKS: RESCIND SPRING BREAKUP LIMITS\n BEGIN NORMAL WEIGHT LIMITS\n'.format(date_string))
            output_str += '{} CTI > 25 + 8-WEEKS: RESCIND SPRING BREAKUP LIMITS\n BEGIN NORMAL WEIGHT LIMITS\n'.format(date_string)
            normal_limits_restarted = True
            overweight_start_date = pd.date_range(start=date_string, periods=14)[-1].strftime("%Y-%m-%d")
            df.at[i, 'message'] = 'CTI > 25 + 8-WEEKS: RESCIND SPRING BREAKUP LIMITS\n BEGIN NORMAL WEIGHT LIMITS'
        
        if not overweight_started and normal_limits_restarted and date_string == overweight_start_date:
            print('{} NORMAL WEIGHT LIMITS + 2 WEEKS: BEGIN OVERWEIGHT PERMITS\n'.format(date_string))
            output_str += '{} NORMAL WEIGHT LIMITS + 2 WEEKS: BEGIN OVERWEIGHT PERMITS\n'.format(date_string)
            overweight_started = True
            df.at[i, 'message'] = 'NORMAL WEIGHT LIMITS + 2 WEEKS: BEGIN OVERWEIGHT PERMITS'

    reduced_df = df[['day','average','roadway_status','message']]
    creation_date = date.today().strftime('%m-%d-%Y')

    # Adds metadata to the tope of the message column
    reduced_df.at[0, 'message'] = creation_date
    reduced_df.at[1, 'message'] = '%.6f_%.6f' % (lat, lon)
    reduced_df.at[2, 'message'] = point_location
    # reduced_df.to_csv(file_path, index=False)
    # print('Breakup prediction csv saved: {}'.format(file_path))
    # output_str += 'Breakup prediction csv saved: {}\n'.format(pathlib.Path(file_path).parent.absolute().__str__()+'\\{}'.format(file_path))
    # output_str += 'Breakup prediction csv saved: {}\n'.format(file_path)

    return reduced_df

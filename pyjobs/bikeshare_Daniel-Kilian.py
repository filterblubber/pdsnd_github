#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Wed Dec 21 16:31:09 2022

@author: Daniel Kilian
"""

import sys
from time import time
import pandas as pd
import numpy as np
import re
from os.path import join, exists, splitext, isfile
from os import listdir
from datetime import timedelta

pd.set_option('display.max_columns', 20)

CITY_DATA = { 'chicago': 'chicago.csv',
              'new york city': 'new_york_city.csv',
              'washington': 'washington.csv' }

data_source = './data'
db_filename = 'bike_database.feather'
db_file = join(data_source, db_filename)

get_statistics = True

dict_months = {1: ('january', 1),
               2: ('february', 2),
               3: ('march', 3),
               4: ('april', 4),
               5: ('may', 5),
               6: ('june', 6),
               0: ('all', list(range(1,7)))}

dict_weekdays = {1: ('monday', 0),
                 2: ('tuesday', 1),
                 3: ('wednesday', 2),
                 4: ('thursday', 3),
                 5: ('friday', 4),
                 6: ('saturday', 5),
                 7: ('sunday', 6),
                 0: ('all', list(range(7)))}


def dict_city(path_in):
    """ Generate city dictionary. """
    file_list = [file 
                 for file in map(str.lower, listdir(path_in)) 
                 if file.endswith('.csv')]

    data_dict = dict()
    for idx, file in enumerate(file_list):
        # print(f'{idx}: {file}')
        base, ext = splitext(file)
        data_dict[idx + 1] = (base, base)
    return data_dict

dict_city = dict_city(data_source)


def select_filter(mydict, filter_select):
    """ Translate user input from dictionay to filter criteria. """
    result = list()
    for item in filter_select:
        temp = mydict.get(item)[1]
        if type(temp).__name__ == 'list':
            for x in temp:
                result.append(x)
        else:
            result.append(temp)
    return sorted(set(result))

        
def dict_question(data_dict):
    """
    Select a item based dictionary.

    Parameters
    ----------
    data_dict : dictionary
        Dictionary with different choices.

    Returns
    -------
    list
        Return a list, based on the selection.

    """
    print('Please choose one or multiple option')
    print('Seperate multiple options with space')
    print('Example(1 2 45)')
    print(40*'-')
    keys = sorted(data_dict.keys())
    while True:    
        for idx in keys:
            value = data_dict[idx][0]
            print(f'{idx}: {value}')
        print(40*'-')
        txt = input('please enter: ').lower().strip()[:200]
        check_for_false_input = re.findall(r'[^0-9 ]', txt)
        if len(check_for_false_input) > 0:
            print('Contains not allowed characters:'
                  f' {set(check_for_false_input)}')
            continue

        txt_split = sorted(map(int, set(txt.split())))

        allowed_choises = [item 
                          for item in txt_split 
                          if item in keys]
        forbidden_choises = [item 
                            for item in txt_split 
                            if item not in keys]

        # print(forbidden_choises, txt_split, keys)
        if len(forbidden_choises) == 0 and len(allowed_choises) > 0:
            return select_filter(data_dict, allowed_choises)
        else:
            print(f'contains wrong choises {forbidden_choises} or is empty')


def calc_time(start_time):
    """
    Function for runtime time measurement.

    Args:
        start_time (float): It's the value of the time() function.
    """
    duration = time() - start_time
    print(f"\nThis took {duration:.2F} seconds.")


def print_table(df, column):
    """
    Print count and percent of the spesified column

    Parameters
    ----------
    df : pands.DataFrame
        DataFrame.
    column : str
        Column name.

    Returns
    -------
    None.

    """
    absolute = df[column].value_counts()
    percent = df[column].value_counts(normalize=True).round(3)
    total = pd.concat([absolute, percent], axis=1)
    total.columns = ['absolute', 'percent']
    print(40*'-')
    print(f'Column {column}')
    print(15*'- ')
    print(total)


def display_duration(duration):
    """
    Dispaly duration of bike rides for [years], days, hh:mm:ss.

    Args:
        duration (int): trip duration of bikes in seconds.
    """

    days_tmp, seconds = divmod(duration, 24*3600)
    years, days = divmod(days_tmp, 365)
    td_sum = timedelta(seconds=int(seconds))

    if years > 0:
        print(f'years: {years}, days: {days}, {td_sum} [hh:mm:ss]')
    elif days > 0:
        print(f'days: {days}, {td_sum} [hh:mm:ss]')
    else:
        print(f'{td_sum} [hh:mm:ss]')


def get_filters(test=False):
    """
    Asks user to specify a city, month, and day to analyze.

    Args:
        test (bool, optional): run function in testing mode. 
            Does not require userinput. Defaults to False.

    Returns:
        dict: returns a dictionary with a key for city, month and day
            with a list of selected filters
    """

    if test:
        filter_dict = {'City':['washington'], 
                          'month': [1, 2, 3, 4, 5, 6], 
                          'dayofweek':[0, 1, 2, 3, 4, 5, 6]}
    else:
        selection_city = dict_question(dict_city)
        selection_months = dict_question(dict_months)
        selection_weekdays = dict_question(dict_weekdays)

        filter_dict = {'City':selection_city,
                         'month': selection_months,
                         'dayofweek':selection_weekdays}

        print(filter_dict)

        print('*'*50)
    return filter_dict



def load_data(df, filter_dict):
    """
    Loads data for the specified city and filters by month and day if applicable.

    initial task
    (str) city - name of the city to analyze
    (str) month - name of the month to filter by, or "all" to apply no month filter
    (str) day - name of the day of week to filter by, or "all" to apply no day filter
    
    Args:
        filter_dict (dict): output dict from get_filter.    
        
    Returns:
        df - Pandas DataFrame containing city data filtered by month and day
    """
 
    result = list()
    for filter_key in ['month', 'dayofweek', 'City']:
        result_tmp = [(df[filter_key] == value).to_numpy() 
                          for value in filter_dict[filter_key]]
        result.append(np.any(result_tmp, axis=0))

    result = np.all(result, axis=0)

    
    return df.loc[result]


def prepare_dataframe(data_source):
    """
    Turn csv into parquet.

    Parameters
    ----------
    data_source : str
        Where is the location of the csv files.

    Returns
    -------
    df_all : pandas.DataFrame
        Return the joined DataFrame.

    """
    file_in = join(data_source, 'chicago.csv')
    check_file_exists(file_in)
    df_c = pd.read_csv(file_in, index_col=0)
    df_c['City'] = 'chicago'

    file_in = join(data_source, 'new_york_city.csv')
    check_file_exists(file_in)
    df_n = pd.read_csv(file_in, index_col=0)
    df_n['City'] = 'new_york_city'

    file_in = join(data_source, 'washington.csv')
    check_file_exists(file_in)
    df_w = pd.read_csv(file_in, index_col=0)
    df_w['City'] = 'washington'
    df_w['Trip Duration'] = df_w['Trip Duration'].round(0).astype(int)

    all_df = [df_c, df_n, df_w]


    df_all = pd.concat(all_df).reset_index(drop=True)
    
    df_all['Start/End Station'] = (df_all['Start Station'] 
                                   + ' / ' 
                                   + df_all['End Station'])

    df_all['Start Time'] = pd.to_datetime(df_all['Start Time'])
    df_all['End Time'] = pd.to_datetime(df_all['End Time'])
    df_all['User Type'] = df_all['User Type'].astype('category')
    df_all['Gender'] = df_all['Gender'].astype('category')
    df_all['City'] = df_all['City'].astype('category')
    df_all['Trip Duration'] = pd.to_numeric(df_all['Trip Duration'], 
                                            downcast='integer')


    df_all['Birth Year'] = pd.to_numeric(df_all['Birth Year'].fillna(-1), 
                                            downcast='integer')

    df_all['month'] = pd.to_numeric(df_all['Start Time'].dt.month,
                                  downcast='integer')
    df_all['dayofweek'] = pd.to_numeric(df_all['Start Time'].dt.dayofweek,
                                      downcast='integer')
    df_all['starthour'] = pd.to_numeric(df_all['Start Time'].dt.hour,
                                      downcast='integer')

    return df_all

    
def time_station_stats(df):
    """Displays statistics on the most frequent times of travel.
       Displays statistics on the most popular stations and trip.
    """

    time_stat_list = [('month', dict_months, 'month'), 
                      ('dayofweek', dict_weekdays, 'day of week'), 
                      ('starthour', None, 'hour'),
                      ('Start Station', None, 'station'),
                      ('End Station', None, 'station'),
                      ('Start/End Station', None, 'station combination')]

    print('\nCalculating The Most Frequent')
    print('Times of Travel and Popular Stations and Trip...\n')
    start_time = time()

    for col, dictionary, name in time_stat_list:
        result = df[col].value_counts().head(1)
        index = result.index[0]
        if dictionary is not None:
            value = dictionary.get(index)[0]
        else:
            value = index
        print(f'The most common {name} for column "{col}" is "{value}".')


    calc_time(start_time)
    print('*'*50)


def trip_duration_stats(df):
    """Displays statistics on the total and average trip duration."""

    print('\nCalculating Trip Duration...\n')
    start_time = time()

    duration_sum = df['Trip Duration'].sum()
    duration_mean = int(df['Trip Duration'].mean())
    
    print(15*'- ')
    print('display total travel time')
    display_duration(duration_sum)
    print(15*'- ')
    print('display mean travel time')
    display_duration(duration_mean)
    print(15*'- ')


    calc_time(start_time)
    print('*'*50)
    
def catch_empty(input_list: list, pos: int):
    if input_list:
        return input_list[pos]
    else:
        return -1


def user_stats(df):
    """Displays statistics on bikeshare users."""

    print('\nCalculating User Stats...\n')
    start_time = time()
    
    print_table(df, 'User Type')
    print_table(df, 'Gender')
    
    # -------------------------------------
    print(40*'-')
    
    mask_ok = df['Birth Year'] != -1
    df_ok = df.loc[mask_ok]


    birth_year = df_ok['Birth Year']
    
    tmp_list = birth_year.value_counts().head(1).index.tolist()
    most_common_birthy = catch_empty(tmp_list, 0)
    
    # -------------------------------------
    max_birth_year = birth_year.max()
    if max_birth_year is np.nan:
        youngest_birthy = -1
    else:
        youngest_birthy = max_birth_year
        
    # -------------------------------------

    mask_most_recent = (df_ok['Start Time'] == 
                        df_ok['Start Time'].max())

    tmp_list = birth_year.loc[mask_most_recent].tolist()
    most_recent_birthy = catch_empty(tmp_list, 0)

    print(f'most common birthyear {most_common_birthy}')
    print(f'youngest birthyear {youngest_birthy}')
    print(f'most recent birthyear {most_recent_birthy}')


    calc_time(start_time)
    print('*'*50)
    
def boolean_question(question_str):
    """
    Ask a question and return a boolean.

    Parameters
    ----------
    question_str : str
        The Question to ask.

    Returns
    -------
    bool
        return yes or no as boolean.

    """
    while True:
        txt = input(f'{question_str} ([y]es/[n]o): ').lower().strip()[:200]
        if txt in ['yes', 'y']:
            print('The Answer is yes')
            return True
        elif txt in ['no', 'n']:
            
            return False
        else:
            print('Answer not recogniced')


def show_raw_data(df_input, rows=5):
    """ Show raw data. """
    lines = 0
    max_lines = len(df_input)
    print(50*'*')
    print('show raw data')
    print(40*'-')
    while True:
        print(df_input.iloc[lines:lines+rows,:-3])
        lines += rows
        question_string = '\nWould you like to continue?\n'
        if lines >= max_lines:
            print(40*'-')
            print('No more data')
            print(40*'-')
            break
        elif not boolean_question(question_string):
            break
    print(50*'*')


def check_file_exists(file_loc):
    if not isfile(file_loc):
        print(f'ERROR - File not found: {file_loc}')
        exit(-1)


def main():
    print('Hello! Let\'s explore some US bikeshare data!')
    print('*'*50)
    print('prepare data')
    t0 = time()

    if not isfile(db_file):
        df = prepare_dataframe(data_source)
        if 'pyarrow' in sys.modules.keys():
            try:
                df.to_feather(db_file, 
                              compression='zstd')
            except Exception as e:
                print(e)
    else:
        df = pd.read_feather(db_file)
    calc_time(t0)

    while True:
        filters = get_filters(test=False)
        df_filtered = load_data(df, filters)
        rows, cols = df_filtered.shape


        if len(df_filtered) > 0 and get_statistics:
            time_station_stats(df_filtered)
            trip_duration_stats(df_filtered)
            user_stats(df_filtered) 
        elif len(df_filtered) == 0:
            print(f'ERROR: no observations ROWS: {rows} / COLUMNS: {cols}')

        print(f'Total observations: {rows}')

        question_string = '\nWould you like to see the raw data?\n'
        if boolean_question(question_string):
            show_raw_data(df_filtered)

        question_string = '\nWould you like to restart the program?\n'
        if not boolean_question(question_string):
            break
        # break
    print('Bye bye')


if __name__ == "__main__":
	main()

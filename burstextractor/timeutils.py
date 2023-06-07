import datetime

import numpy as np
import pandas as pd


def extract_time(data):
    ## Fix typos in the data
    # Sometimes, the time range sometimes uses a : instead of a -. Thus, it's hard to split the time range
    extracted_digits = data['time'].str.extract(r'(\d+).(\d+).(\d+).(\d+)', expand=True)
    
    data['time'] = extracted_digits[0] + ':' + extracted_digits[1] + '-' + extracted_digits[2] + ':' + extracted_digits[3]
    data['time_start'] = extracted_digits[0] + ':' + extracted_digits[1]
    data['time_end'] = extracted_digits[2] + ':' + extracted_digits[3]

    return data

def fix_typos_in_time(data):
    replace_dict = {
        '06:06-06:88': '06:06-06:08',
        '24:32-14:33': '14:32-14:33',
        '21:18-212:19': '21:18-21:19',
    }
    
    data['time'] = data['time'].replace(replace_dict)
    return data

def fix_24_hour_time(data):
    """
    The time range sometimes uses a 24:00 instead of 00:00. Thus, it's hard to split the time range
    """
    data['date_start'] = data['date']
    data['date_end'] = np.where(data['time_end'] == '24:00', (data['date'].astype(int) + 1).astype(str), data['date'])
    
    for time in ['time', 'time_start', 'time_end']:
        data[time] = data[time].str.replace('24:00', '00:00')
    
    return data

def create_datetime(data):
    data['datetime_start'] = pd.to_datetime(data['date_start'] + ' ' + data['time_start'], format='%Y%m%d %H:%M')
    data['datetime_end'] = pd.to_datetime(data['date_end'] + ' ' + data['time_end'], format='%Y%m%d %H:%M')
    return data

def check_valid_date(year, month):
    """
    Check if the argument to the funtion download_burst_list
    is a valid date. If not, aises an exception.
    """
    assert (
        len(str(year)) == 4 and type(year) == int
    ), "First argument year must be a 4-digit integer"
    assert type(month) == int, "Second argument month must be a valid integer"
    assert month >= 1 and month <= 12, "Second argument month must be from 1 to 12"
    if datetime.date.today().year == year:
        assert (
            datetime.date.today().month >= month
        ), "The month {} in {} has not yet occurred".format(month, year)
    assert (
        datetime.date.today().year >= year
    ), "The year {} has not yet occurred".format(year)


def adjust_year_month(year, month):
    """
    We'll work with string numbers in function download_burst_list.
    Here we convert the arguments to strings and pads the month
    to length 2, if necessary.
    Returns: the year and month as strings
    """
    if month < 10:
        m = "0" + str(month)
    else:
        m = str(month)

    return str(year), m
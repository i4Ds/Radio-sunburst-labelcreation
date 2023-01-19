"""
Reads a burst list compiled by C. Monstein from server and processes its data
version 1.3
author: Andreas Wassmer
project: Raumschiff
"""
import pandas as pd
from burstextractor import timeutils
import requests
import os

BASE_URL = f"http://soleil.i4ds.ch/solarradio/data/BurstLists/2010-yyyy_Monstein"
ENCODING = "iso-8859-1"

def process_burst_list(filename):
    """
    Let's discard the entries with missing data.
    These events have a time stamp of "##:##-##:##" with no further data in the row except the date
    I like to use a conditional for filtering. I think the filter is more readable.
    Especially if there are several conditions
    Returns: A Pandas Dataframe with valid events
    """
    col_names = ['date', 'time', 'type', 'instruments']
    skip_row_idxs = []
    with open(filename, "r") as f:
        for row_idx, line in enumerate(f):
            if (not line.startswith("20")) or len(line) < 12 or '##:##-##:##' in line or '??' in line:
                skip_row_idxs.append(row_idx)
                
    data = pd.read_csv(filename, sep="\t", index_col=False, encoding=ENCODING, names=col_names, engine="python", skiprows=skip_row_idxs, dtype=str)
    
    return data


def download_burst_list(select_year, select_month, folder='ecallisto_files'):
    """
    The burst list contains all (manually) detected radio bursts per
    month and year. This function gets the file from the server.
    Returns: the filename of the list.
             I decided not to return the content but rather the
             location of the file. This keeps the data for further
             processing with other tools if needed.
    """
    timeutils.check_valid_date(select_year, select_month)
    year, month = timeutils.adjust_year_month(select_year, select_month)

    filename = os.path.join(folder, 'e-CALLISTO_{year}_{month}.txt')
    flare_list = requests.get(f"{BASE_URL}/{year}/{filename}")
    with open(filename, "w") as f:
        f.write(flare_list.content.decode(ENCODING))
    return 
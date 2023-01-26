# Download all spectograms with burst in the corresponding folder 
import os
from tqdm import tqdm
from multiprocessing.pool import Pool as Pool
import pickle
from burstextractor.burstlist import download_burst_list, process_burst_list
from burstextractor.timeutils import extract_time, fix_typos_in_time, fix_24_hour_time, create_datetime, check_valid_date, adjust_year_month
from burstextractor.data_utils import explode_instruments_long_clean_instruments, keep_only_type_I_to_VI
from spectogram_utils import spec_to_pd_dataframe, plot_spectogram
import pandas as pd
import numpy as np
import datetime
import matplotlib.pyplot as plt 
from radiospectra.sources import CallistoSpectrogram
import requests
from bs4 import BeautifulSoup
from tqdm import tqdm
from datetime import datetime, timedelta

LOCAL_DATA_FOLDER = os.path.join(os.path.abspath(os.sep), 'var', 'lib', 'ecallisto')
FILES_BASE_URL = 'http://soleil.i4ds.ch/solarradio/data/2002-20yy_Callisto/'
MIN_FILE_SIZE = 2000 # Minimum file size in bytes, to redownload empty files

def fetch_content(url):
    reqs = requests.get(url)
    soup = BeautifulSoup(reqs.text, 'html.parser')
    return soup

def extract_content(soup, substrings_to_include, substring_to_exclude):
    """
    Extracts all the content from the given soup object based on the given parameters
    substrings_to_include: If specified, only links with the given substrings will be extracted
    substrings_to_exclude: If specified, links with the given substrings will be excluded
    
    Returns a list of all the links
    """
    content = []
    for link in soup.find_all('a'):
        if all([pattern in link.get('href') for pattern in substrings_to_include]): # If all of the substrings are in the link
            if not any([pattern in link.get('href') for pattern in substring_to_exclude]): # If none of the substrings are in the link
                content.append(link.get('href'))
    return content

def extract_fit_gz_files(url, instrument, substrings_to_include=None, substring_to_exclude=None):
    """
    Extracts all the .fit.gz files from the given url
    instrument: If specified, only files with the instrument name will be extracted
    substrings_to_include: If specified, only files with the given substrings will be extracted
    substring_to_excluce: If specified, files with the given substrings will be excluded
    
    Returns a list of all the .fit.gz files
    """
    soup = fetch_content(url)
    if substrings_to_include is None:
        substrings_to_include = ['.fit.gz']
    if substring_to_exclude is None:
        substring_to_exclude = []
    if instrument:
        substrings_to_include.append(instrument)
    return extract_content(soup, substrings_to_include = substrings_to_include, substring_to_exclude=substring_to_exclude)

def extract_fiz_gz_files_urls(year, month, day, instrument):
    """
    Extracts all the .fit.gz files from the given year, month and day
    instrument: If specified, only files with the instrument name will be extracted
    
    Returns a list of all the .fit.gz files
    """
    url = f'{FILES_BASE_URL}{year}/{month}/{day}/'
    file_names = extract_fit_gz_files(url, instrument=instrument)
    urls = [url + file_name for file_name in file_names]
    return urls

def download_ecallisto_file(URL, return_download_path=False, root=LOCAL_DATA_FOLDER):
    # Split URL to get the file name and add the directory
    year, month, day, filename = URL.split('/')[-4:]
    directory = os.path.join(root, year, month, day)
    os.makedirs(directory, exist_ok=True)
    
    # Check if the file already exists
    file_path = os.path.join(directory, filename)
    if not os.path.exists(file_path) or os.path.getsize(file_path) < MIN_FILE_SIZE: # Check that it is not an empty file (e.g. 404 error)
        # Downloading the file by sending the request to the URL
        req = requests.get(URL)
        with open(file_path,'wb') as output_file:
            output_file.write(req.content)
    # Return path (e.g. for astropy.io.fits.open)
    if return_download_path:
        return file_path
    
def download_ecallisto_files(start_date=datetime.now().date() - timedelta(days=1), end_date=datetime.today().date(), instrument='ALASKA', return_download_paths=False, root=LOCAL_DATA_FOLDER):
    """
    Downloads all the eCallisto files from the given start date to the end date. If the files already exist, they will not be downloaded again.
    start_date: Start date of the download (datetime object). If empty, it will be set to yesterday
    end_date: End date of the download (datetime object). If empty, it will be set to today
    instrument: If specified, only files with the instrument name will be extracted (substring)
    
    Returns None
    """
    assert start_date < end_date, 'Start date should be less than end date and both should be datetime objects'
    paths = []
    for year in range(start_date.year, end_date.year + 1):
        for month in range(start_date.month, end_date.month + 1):
            month = f'0{month}' if month < 10 else month
            for day in range(start_date.day, end_date.day + 1):
                day = f'0{day}' if day < 10 else day
                urls = extract_fiz_gz_files_urls(year, month, day, instrument=instrument)
                for url in tqdm(urls, desc=f'Downloading {year}-{month}-{day}'):
                    path = download_ecallisto_file(url, return_download_path=True)
                    if return_download_paths:
                        paths.append(path)
    if return_download_paths:
        return paths
                
if __name__ == '__main__':
    print(f'Downloading files to {LOCAL_DATA_FOLDER}')
    download_ecallisto_files(start_date=datetime(2023, 1, 1), end_date=datetime(2023, 1, 2), instrument='ALASKA')

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


def download_save_spectogram(burst_list_row):
    path = os.path.join(IMAGE_FOLDER, burst_list_row['type'])
    os.makedirs(path, exist_ok=True)
    spec = download_spectogram_from_df_row(burst_list_row, IMAGE_LEN)
    file_path = os.path.join(path, f'{burst_list_row["instruments"]}-{burst_list_row["date"]}-{burst_list_row["time"]}.pkl')
    pickle.dump(spec, open(file_path, 'wb'))
    
    
    
    
if __name__ == '__main__':
    import sys
    if len(sys.argv) > 1:
        N = int(sys.argv[1])
    IMAGE_LEN = pd.Timedelta('1m')
    IMAGE_FOLDER = 'images'
    
    burst_list = pd.read_excel('burst_list.xlsx')
    print(burst_list.sample(5))
from database_functions import *
from database_utils import *
import argparse
from data_creation import LOCAL_DATA_FOLDER
from datetime import datetime, timedelta
from tqdm import tqdm
from radiospectra.sources import CallistoSpectrogram
from spectogram_utils import masked_spectogram_to_array, spec_time_to_pd_datetime
import numpy as np 

def main(start_date, end_date, instrument="all", dir=LOCAL_DATA_FOLDER, logger=None):

    path = glob_files(start_date, end_date, instrument, dir)
    dict_paths = create_dict_of_instrument_paths(path)
    
    t = tqdm(dict_paths, total=len(dict_paths))
    for instrument in t:
        if instrument not in get_table_names_sql():
            # Get random file path to get meta data and create table
            file_path  = dict_paths[instrument][0]
            add_instrument_from_path_to_database(file_path)
        for path in dict_paths[instrument]:
            add_spec_from_path_to_database(path)
            
if __name__ == "__main__":
    ## Example:
    # python load_data_into_database.py --start_date 2020-01-01 --end_date 2020-01-02 --instrument all
    # Get arguments from command line
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--start_date",
        type=str,
        default=(datetime.today().date() - timedelta(days=14)))
    parser.add_argument(
        "--end_date",
        type=str,
        default=datetime.today().date())
    parser.add_argument(
        "--instrument",
        type=str,
        default="all")
    parser.add_argument(
        "--dir",
        type=str,
        default=LOCAL_DATA_FOLDER)
    args = parser.parse_args()
    # Update to correct types
    args.start_date = datetime.strptime(args.start_date, "%Y-%m-%d").date()
    args.end_date = datetime.strptime(args.end_date, "%Y-%m-%d").date()
    # Main
    main(**vars(args))
    
    
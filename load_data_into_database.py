from database_functions import *
from database_utils import *
import argparse
from data_creation import LOCAL_DATA_FOLDER
from datetime import datetime, timedelta
from tqdm import tqdm
from multiprocessing.pool import Pool as Pool
import logging_utils

LOGGER = logging_utils.setup_custom_logger("database_data_addition")

def main(start_date, end_date, instrument, dir):
    paths = glob_files(dir_path=dir, start_date=start_date, end_date=end_date, file_name_pattern=instrument)
    dict_paths = create_dict_of_instrument_paths(paths)
    
    t = tqdm(dict_paths.keys(), desc="Adding instruments to database", position=0)
    for instrument in t:
        if instrument not in get_table_names_sql():
            # Get random file path to get meta data and create table
            file_path = dict_paths[instrument][0]
            add_instrument_from_path_to_database(file_path)
        with Pool() as p:
            r = list(tqdm(p.imap(add_spec_from_path_to_database, dict_paths[instrument]), total=len(dict_paths[instrument]), desc="Adding specs to database", position=1))
    LOGGER.info(f"Done adding data to database. {len(dict_paths)} rows added.")

if __name__ == "__main__":
    ## Example:
    # python load_data_into_database.py --start_date 2020-01-01 --end_date 2020-01-02 --instrument_glob_pattern *
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
        "--instrument_glob_pattern",
        type=str,
        default="*")
    parser.add_argument(
        "--dir",
        type=str,
        default=LOCAL_DATA_FOLDER)
    args = parser.parse_args()
    LOGGER.info(f"Adding data from {args.start_date} to {args.end_date} to database. Args: {args}")
    # Update date to datetime
    args.start_date = datetime.strptime(args.start_date, "%Y-%m-%d").date()
    args.end_date = datetime.strptime(args.end_date, "%Y-%m-%d").date()
    try:
        # Main
        main(**vars(args))
    except Exception as e:
        LOGGER.exception(e)
        raise e
    LOGGER.info("Done")
    
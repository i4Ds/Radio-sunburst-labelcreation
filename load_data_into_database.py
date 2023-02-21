import argparse
import multiprocessing as mp
import os
from datetime import datetime, timedelta
from multiprocessing.pool import Pool as Pool

from tqdm import tqdm

import logging_utils
from data_creation import get_urls
from database_functions import *
from database_utils import *

LOGGER = logging_utils.setup_custom_logger("database_data_addition")
from threading import Thread


def main(start_date, end_date, instrument_substring):
    urls = get_urls(start_date, end_date, instrument_substring)
    dict_paths = create_dict_of_instrument_paths(urls)

    for instrument in dict_paths.keys():
        if instrument not in get_table_names_sql():
            # Get random file path to get meta data and create table
            file_path = dict_paths[instrument][0]
            add_instrument_from_path_to_database(file_path)

    with mp.Pool(os.cpu_count()) as pool:
        pool.imap_unordered(add_spec_from_path_to_database, tqdm(urls, total=len(urls)))

        # Wait for all tasks to complete
        pool.close()
        pool.join()

    # Wait for the progress bar update thread to finish

    LOGGER.info(f"Done adding data to database. {len(urls)} files added.")


if __name__ == "__main__":
    ## Example:
    # python load_data_into_database.py --start_date 2022-01-01 --end_date 2023-02-20 --instrument_substring None
    # Get arguments from command line
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--start_date",
        type=str,
        default=(datetime.today().date() - timedelta(days=1)).strftime("%Y-%m-%d"),
    )
    parser.add_argument(
        "--end_date", type=str, default=datetime.today().date().strftime("%Y-%m-%d")
    )
    parser.add_argument(
        "--instrument_substring",
        type=str,
        default="None",
        help="Instrument glob pattern. Default is 'None', which means all instruments (no instrument substring pattern matching).",
    )
    args = parser.parse_args()
    LOGGER.info(
        f"Adding data from {args.start_date} to {args.end_date} to database. Args: {args}"
    )
    # Update date to datetime
    args.start_date = datetime.strptime(args.start_date, "%Y-%m-%d").date()
    args.end_date = datetime.strptime(args.end_date, "%Y-%m-%d").date()
    # Update instrument glob pattern to all if needed
    args.instrument_substring = (
        "all"
        if args.instrument_substring.lower() in ["None"]
        else args.instrument_substring
    )
    try:
        # Main
        main(**vars(args))
    except Exception as e:
        LOGGER.exception(e)
        raise e
    LOGGER.info("Done")

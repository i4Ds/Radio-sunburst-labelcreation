import argparse
from datetime import datetime, timedelta
from multiprocessing.pool import Pool as Pool

from tqdm import tqdm

import logging_utils
from data_creation import get_urls
from database_functions import *
from database_utils import *

LOGGER = logging_utils.setup_custom_logger("database_data_addition")


def main(start_date, end_date, instrument_substring):
    urls = get_urls(start_date, end_date, instrument_substring)
    dict_paths = create_dict_of_instrument_paths(urls)

    t = tqdm(dict_paths.keys(), desc="Adding instruments to database", position=0)
    for instrument in t:
        if instrument not in get_table_names_sql():
            # Get random file path to get meta data and create table
            file_path = dict_paths[instrument][0]
            add_instrument_from_path_to_database(file_path)
        with Pool() as p:
            r = list(
                tqdm(
                    p.imap(add_spec_from_path_to_database, dict_paths[instrument]),
                    total=len(dict_paths[instrument]),
                    desc="Adding specs to database",
                    position=1,
                )
            )
    LOGGER.info(f"Done adding data to database. {len(dict_paths)} rows added.")


if __name__ == "__main__":
    ## Example:
    # python load_data_into_database.py --start_date 2022-01-01 --end_date 2023-02-20 --instrument_substring None
    # Get arguments from command line
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--start_date", type=str, default=(datetime.today().date() - timedelta(days=14))
    )
    parser.add_argument("--end_date", type=str, default=datetime.today().date())
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

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


def main(start_date, instrument_substring, chunk_size, cpu_count):
    # Check data for today
    today = datetime.today().date()
    urls = get_urls(start_date, end_date, instrument_substring)
    dict_paths = create_dict_of_instrument_paths(urls)

    for instrument in dict_paths.keys():
        if instrument not in get_table_names_sql():
            for path in dict_paths[instrument]:
                # Try to add the instrument to the database
                # Sometimes it fails because the file is corrupted. In that case, try the next file and break if it works
                try:
                    add_instrument_from_path_to_database(path)
                except Exception as e:
                    LOGGER.error(f"Error adding instrument {instrument}: {e}")
                else:
                    break

    with mp.Pool(cpu_count) as pool:
        pool.map_async(
            add_spec_from_path_to_database,
            tqdm(urls, total=len(urls)),
            chunksize=chunk_size,
        )

        # Wait for all tasks to complete
        pool.close()
        pool.join()

    # Wait for the progress bar update thread to finish

    LOGGER.info(f"Done adding data to database. {len(urls)} files added.")


if __name__ == "__main__":
    ## Example:
    # python load_data_into_database.py --start_date 2022-01-01 --end_date 2023-02-20
    # --instrument_substring "Australia-ASSA, Arecibo-Observatory, HUMAIN, SWISS-Landschlacht, ALASKA-COHOE"
    # The parameter for the multiprocessing is optimized via cprofiler.
    # Get arguments from command line
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--start_date",
        type=str,
        default=(datetime.today().date() - timedelta(days=1)).strftime("%Y-%m-%d"),
    )
    parser.add_argument(
        "--instrument_substring",
        type=str,
        default="None",
        help="Instrument glob pattern. Default is 'None', which means all instruments currently in the database and every newly added instrument \
        (added in the last two days). Accepts also a list of instruments, e.g. 'Australia-ASSA, Arecibo-Observatory, HUMAIN, SWISS-Landschlacht, ALASKA-COHOE' \
        If you pass a List, only those instruments are updated and the ones added in the last two days are added.",
    )
    parser.add_argument(
        "--date_range_chunk_size",
        type=str,
        default="1w",
        help="Date range chunk size. Default is '1w'. This means that it imports data in chunks of one week before going to the next week. ",
    )
    parser.add_argument(
        "--chunk_size",
        type=int,
        default=10,
        help="Chunk size for multiprocessing. Default is 10.",
    )
    parser.add_argument(
        "--cpu_count",
        type=int,
        default=os.cpu_count(),
        help="Number of CPUs to use. Default is all available CPUs.",
    )
    args = parser.parse_args()
    LOGGER.info(f"Adding data from {args.start_date}. Args: {args}")
    # Update date to datetime
    args.start_date = datetime.strptime(args.start_date, "%Y-%m-%d").date()
    # Update instrument glob pattern to all if needed or convert to list
    args.instrument_substring = (
        args.instrument_substring if args.instrument_substring != "None" else None
    )
    if args.instrument_substring is not None:
        args.instrument_substring = args.instrument_substring.split(", ")
    try:
        # Main
        main(**vars(args))
    except Exception as e:
        LOGGER.exception(e)
        raise e
    LOGGER.info("Done")

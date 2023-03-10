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


def add_instruments_from_paths_to_database(dict_paths):
    """
    Add instruments from paths to database. If the instrument is already in the database, it will not be added.
    Parameters
    ----------
    dict_paths : dict
        Dictionary of instrument paths.
    """
    # Add the instruments to the database
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


def add_specs_from_paths_to_database(urls, chunk_size, cpu_count):
    with mp.Pool(cpu_count) as pool:
        pool.map_async(
            add_spec_from_path_to_database,
            tqdm(urls, total=len(urls)),
            chunksize=chunk_size,
        )

        # Wait for all tasks to complete
        pool.close()
        pool.join()


def main(
    start_date: datetime.date,
    instrument_substring: str,
    days_chunk_size: int,
    chunk_size: int,
    cpu_count: int,
) -> None:
    """
    Add instrument data to a database.

    Parameters
    ----------
    start_date : datetime.date
        The starting date for adding instrument data to the database.
    instrument_substring : str
        A substring to match instrument names with.
    days_chunk_size : int
        The number of days of instrument data to add to the database at once.
    chunk_size : int
        The number of instrument data files to add to the database at once.
    cpu_count : int
        The number of CPU cores to use when adding instrument data to the database.

    Returns
    -------
    None

    Notes
    -----
    This function iteratively adds instrument data to a database. First, it creates a database using the data for
    the current day and the previous day. Then, it adds the data to the database. Next, it iterates over all tables in
    the database, and for each table it adds data for the previous `days_chunk_size` days. The function stops when no
    new data is added to the database.

    Examples
    --------
    To add instrument data to a database for instruments containing the substring 'ALASKA-COHOE' starting from
    January 1st, 2023 with a days chunk size of 30, a chunk size of 100, and 8 CPU cores, you could run:

    >>> start_date = datetime.date(2023, 1, 1)
    >>> instrument_substring = 'ALASKA-COHOE'
    >>> days_chunk_size = 30
    >>> chunk_size = 100
    >>> cpu_count = 8
    >>> main(start_date, instrument_substring, days_chunk_size, chunk_size, cpu_count)
    """
    while True:
        # # Check data for today and yesterday to create the database.
        # LOGGER.info("Checking data for today and yesterday to create the database.")
        today = datetime.today().date()
        # urls = get_urls(today - timedelta(days=1), today, instrument_substring)
        # dict_paths = create_dict_of_instrument_paths(urls)
        # LOGGER.info(f"Found {len(urls)} files for today and yesterday.")
        # # Add the instruments to the database
        # add_instruments_from_paths_to_database(dict_paths)

        # # Add the data to the database
        # add_specs_from_paths_to_database(urls, chunk_size, cpu_count)

        # Iteratively add data per instrument to the database
        specs_added = {}
        urls = []
        for table in get_table_names_sql():
            LOGGER.info(f"Checking data for {table}.")
            try:
                # Get the newest date in the database
                max_date = get_max_date_of_table(table).date()

                # Get name of the fits file
                glob_pattern = [reverse_extract_instrument_name(table).lower()]

                # If the newest date is today, then the data is already up to date and we can check that the data in the end of the database is up to date.
                if not max_date == today:
                    urls.extend(
                        get_urls(
                            max_date,
                            max_date + timedelta(days=days_chunk_size),
                            glob_pattern,
                        )
                    )

                # Get the last date in the database
                min_date = get_min_date_of_table(table).date()

                # Get the urls
                urls.extend(
                    get_urls(
                        min_date - timedelta(days=days_chunk_size),
                        min_date,
                        glob_pattern,
                    )
                )

                # Log the number of files added
                LOGGER.info(
                    f"Adding {len(urls)} files to {table} between {min_date - timedelta(days=days_chunk_size)} and {min_date}."
                )

                # Add the data to the database
                add_specs_from_paths_to_database(urls, chunk_size, cpu_count)
                specs_added[table] = len(urls)
            except ImportError as e:
                LOGGER.error(f"Error adding data to {table}: {e}")
                specs_added[table] = 0
        # Stop if no data was added to the database
        if sum(specs_added.values()) == 0 or len(specs_added) == 0:
            LOGGER.info("No more data to add to the database.")
            break


if __name__ == "__main__":
    ## Example:
    # python continiously_add_data_to_database.py --start_date 2023-03-01
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
        "--days_chunk_size",
        type=int,
        default=7,
        help="Days chunk size. Default is '7'. This means that it imports data in chunks of one week before going to the next week. Warning: \
        It could also be that it adds 2x the amount of data incase the data in the database is missing the newest data and the oldest data at the same time.",
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
        None if args.instrument_substring == "None" else args.instrument_substring
    )
    if args.instrument_substring is not None and "," in args.instrument_substring:
        args.instrument_substring = args.instrument_substring.split(", ")
    try:
        # Main
        main(**vars(args))
    except Exception as e:
        LOGGER.exception(e)
        raise e
    LOGGER.info("Done")

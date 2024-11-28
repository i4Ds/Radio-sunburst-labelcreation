# %%
from ecallisto_ng.data_download.downloader import get_ecallisto_data
from datetime import datetime, timedelta
import pandas as pd
import os
from tqdm import tqdm

# Define the folder where Parquet files will be saved
FOLDER = "/mnt/nas05/data01/vincenzo/ecallisto/2014"

# Define the list of instruments
INSTRUMENT_FILTER = [
    "BIR",
    "HB9SCT",
    "BLENSW",
    "DARO-HF",
    "ESSEN",
    "GLASGOW",
    "HUMAIN",
    "ROSWELL-NM",
    "RWANDA",
    # Add other instruments as needed
]

# Define the start and end datetime for data retrieval
START_DATE = datetime(2014, 1, 1)
END_DATE = datetime(2014, 12, 31)


def create_overlapping_parquets(start_datetime, end_datetime, instrument_list, folder):
    """
    Create overlapping Parquet files of 15-minute windows overlapping by 1 minute
    for each instrument in the instrument_list between start_datetime and end_datetime.

    Parameters:
    - start_datetime: datetime.datetime
    - end_datetime: datetime.datetime
    - instrument_list: list of str
    - folder: str
    """
    # Ensure the folder exists
    os.makedirs(folder, exist_ok=True)

    # Generate list of dates between start_datetime and end_datetime
    date_list = pd.date_range(start_datetime, end_datetime, inclusive="both")

    for instrument in tqdm(instrument_list, desc="[Instruments]", position=1):
        for date in tqdm(date_list, desc="[Dates]", leave=False, position=2):
            # Define start and end of the day
            day_start = date - timedelta(minutes=15)
            day_end = day_start + timedelta(hours=24, minutes=30)
            # Create overlapping windows
            window_start = day_start

            while window_start < day_end:
                dfs = get_ecallisto_data(
                    window_start,
                    window_start + timedelta(minutes=15),
                    instrument_name=instrument,
                    download_from_local=True,
                )
                for instrument, df in dfs.items():
                    if df is None or df.empty:
                        continue  # No data for this instrument on this day
                    # Make sure that the window is at least 10 minutes long.
                    if df.index.max() - df.index.min() > timedelta(minutes=10):
                        # Define the path to save the Parquet file
                        filename = (
                            f"{window_start.strftime('%Y-%m-%d_%H-%M-%S')}.parquet"
                        )
                        path = os.path.join(folder, instrument, filename)
                        os.makedirs(os.path.dirname(path), exist_ok=True)
                        # Save the window data to Parquet
                        df.to_parquet(path)
                window_start += timedelta(minutes=14)


# Call the function with the defined parameters
create_overlapping_parquets(START_DATE, END_DATE, INSTRUMENT_FILTER, FOLDER)

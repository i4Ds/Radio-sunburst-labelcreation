# %%
from ecallisto_ng.data_download.downloader import get_ecallisto_data
from ecallisto_ng.burst_list.utils import load_burst_list
from datetime import timedelta, datetime
from PIL import Image
import random
import os
from tqdm import tqdm
import pandas as pd

FOLDER = "/mnt/nas05/data01/vincenzo/ecallisto/hu_dataset_live_mai_october"
RESOLUTION = (256, 256)
BURST_NON_BURST_RATIO = 10  # 10: There are 10x more non bust than burst images.
START_DATE = datetime(2024, 5, 13)
INSTRUMENT_FILTER = [
    "MEXICO-FCFM-UANL_01",
    "USA-ARIZONA-ERAU_01",
    "GLASGOW_01",
    "EGYPT-Alexandria_02",
    "BIR_01",
    "ALASKA-HAARP_62",
    "MONGOLIA-UB_01",
    "KASI_59",
    "ALMATY_58",
    "MRO_59",
    "MRO_61",
    "ALGERIA-CRAAG_59",
    "ALASKA-COHOE_63",
    "AUSTRIA-UNIGRAZ_01",
    "Australia-ASSA_02",
    "Australia-ASSA_62",
    "GERMANY-DLR_63",
    "HUMAIN_59",
    "INDIA-GAURI_01",
    "INDIA-OOTY_02",
    "MEXART_59",
    "MEXICO-LANCE-B_62",
    "NORWAY-EGERSUND_01",
    "SSRT_59",
    "SWISS-Landschlacht_62",
    "TRIEST_57",
    "GREENLAND_62",
    "SWISS-HEITERSWIL_59",
    "USA-BOSTON_62",
    "ALASKA-ANCHORAGE_01",
    "INDONESIA_60",
    "ITALY-Strassolt_01",
    "Malaysia-Banting_01",
    "UZBEKISTAN_01",
    "ROMANIA_01",
]


def random_duration(min_start, min_end):
    """
    Generate a random duration between a specified range in minutes.

    Parameters:
    min_start (int): The start of the range in minutes.
    min_end (int): The end of the range in minutes.

    Returns:
    datetime.timedelta: A timedelta object representing the duration.
    """
    # Generate a random duration in minutes (including fractions) within the specified range
    total_minutes = random.randint(min_start, min_end)

    return timedelta(minutes=total_minutes)


def save_image(df, path):
    """
    Save an image to a file.

    Parameters:
    df (pandas.DataFrame): The dataframe containing the image data.
    path (str): The path to save the image to.
    """
    # Get the image data from the dataframe
    image_data = df.values

    # Convert the image data to a PIL Image
    image = Image.fromarray(image_data)

    # Save the image to the specified path
    image.save(path)


def return_random_datetime_between(start_datetime, end_datetime):
    """
    Generate a random datetime between two specified datetimes.

    Parameters:
    start_datetime (datetime.datetime): The start of the range.
    end_datetime (datetime.datetime): The end of the range.

    Returns:
    datetime.datetime: A random datetime between the two specified datetimes.
    """
    # Calculate the total number of seconds between the two datetimes
    total_seconds = (end_datetime - start_datetime).total_seconds()

    # Generate a random number of seconds within the specified range
    random_seconds = random.randint(0, total_seconds)

    # Return the start datetime plus the random number of seconds
    # Rounded to minutes
    return (start_datetime + timedelta(seconds=random_seconds)).replace(
        second=0, microsecond=0
    )


burst_list = load_burst_list("burst_list.xlsx")
date_filtered_burst_list = burst_list[burst_list["datetime_start"] >= START_DATE]

for instrument in tqdm(
    INSTRUMENT_FILTER,
    desc="[Instruments]",
    position=1,
):
    filtered_burst_list = date_filtered_burst_list[
        date_filtered_burst_list["instruments"].isin([instrument.split("_")[0]]).copy()
    ]  #

    # # Some Filtering for specific instruments
    burst_generated = 0

    for i, row in tqdm(
        filtered_burst_list.iterrows(),
        total=filtered_burst_list.shape[0],
        desc=f"[Data]",
        position=2,
    ):
        datetime_start = row["datetime_start"] - random_duration(0, 11)
        end_time = datetime_start + timedelta(minutes=15)
        dfs = get_ecallisto_data(
            datetime_start,
            end_time,
            instrument_name=instrument,
            download_from_local=True,
        )
        for _, df in dfs.items():
            df: pd.DataFrame
            if not df.attrs["FULLNAME"] == instrument:
                continue
            try:
                assert (df.index.max() - df.index.min()) > pd.Timedelta(
                    10, unit="minutes"
                ), "Too short."
                ## Path to save the image to
                # It's FOLDER / instrument / burst type / datetime_start.png
                path = os.path.join(
                    FOLDER,
                    df.attrs["FULLNAME"],
                    str(row["type"]),
                    row["datetime_start"].strftime("%Y-%m-%d_%H-%M-%S") + ".parquet",
                )
                os.makedirs(os.path.dirname(path), exist_ok=True)
                df.to_parquet(path)
                burst_generated += 1
            except Exception as e:
                print(e)
                print(row["datetime_start"])
                print(row["datetime_end"])
                print(row["instruments"])
                print(instrument)
    # ## Non Bursts
    # Machen wir ähnlich, aber halt andersrum.
    non_burst_generated = 0

    min_datetime, max_datetime = (
        date_filtered_burst_list["datetime_start"].min(),
        date_filtered_burst_list["datetime_start"].max(),
    )
    print("Start Datetime:", min_datetime)
    print("End Datetime:", max_datetime)

    # Initialize tqdm with a large total and manually update
    desired_total_count = burst_generated * BURST_NON_BURST_RATIO
    pbar = tqdm(total=desired_total_count, desc=f"[Non Burst data]")
    # Read in the unfiltered burst list.
    # This contains ALL the stations, even if they had wierd typos and bursts of other instruments.
    while non_burst_generated < desired_total_count:
        start_datetime = return_random_datetime_between(min_datetime, max_datetime)
        end_datetime = start_datetime + timedelta(minutes=15)
        # Now we need to check that the start_datetime is not in a burst
        non_burst_in_burst_df = date_filtered_burst_list[
            (
                (date_filtered_burst_list.datetime_start <= start_datetime)
                & (start_datetime <= date_filtered_burst_list.datetime_end)
            )
            | (
                (date_filtered_burst_list.datetime_start <= end_datetime)
                & (end_datetime <= date_filtered_burst_list.datetime_end)
            )
        ]
        if not non_burst_in_burst_df.empty:
            print("Datetime is in a burst, trying again...")
            continue

        dfs = get_ecallisto_data(
            start_datetime,
            end_datetime,
            instrument_name=instrument,
            download_from_local=True,
        )
        for _, df in dfs.items():
            if not df.attrs["FULLNAME"] == instrument:
                continue
            try:
                assert (df.index.max() - df.index.min()) > pd.Timedelta(
                    10, unit="minutes"
                ), "Too short."
                path = os.path.join(
                    FOLDER,
                    df.attrs["FULLNAME"],
                    "0",
                    start_datetime.strftime("%Y-%m-%d_%H-%M-%S") + ".parquet",
                )
                os.makedirs(os.path.dirname(path), exist_ok=True)
                df.to_parquet(path)
                pbar.update(1)  # Increment the progress bar by one
                non_burst_generated += 1
            except Exception as e:
                print(e)
                print(row["datetime_start"])
                print(row["datetime_end"])
                print(row["instruments"])
                print(instrument)

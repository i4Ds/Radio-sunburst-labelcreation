# %%
from ecallisto_ng.data_download.downloader import get_ecallisto_data
from ecallisto_ng.burst_list.utils import load_burst_list
from datetime import timedelta
from PIL import Image
import random
import os
from tqdm import tqdm

FOLDER = "/mnt/nas05/data01/vincenzo/ecallisto/data"
RESOLUTION = (256, 256)
BURST_NON_BURST_RATIO = 5  # 5: There are 5x more non bust than burst images.
resample_delta = timedelta(minutes=15) / RESOLUTION[0]  # Ist nicht perfekt, aber geht
instruments = [
    "USA-ARIZONA-ERAU_01",
    "GLASGOW_01",
    "EGYPT-Alexandria_02",
    "BIR_01",
    "ALASKA-HAARP_62",
    "MEXICO-FCFM-UANL_01",
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


for instrument in instruments:
    burst_list = load_burst_list("burst_list.xlsx")
    # # Some Filtering for specific instruments
    burst_generated = 0
    filtered_burst_list = burst_list[
        burst_list["instruments"].isin([instrument.split("_")[0]]).copy()
    ]  # Burstliste hat nur der Ort der Antenna, aber nicht die ID, darum #pythonmagic

    for i, row in tqdm(
        filtered_burst_list.iterrows(),
        total=filtered_burst_list.shape[0],
        desc=f"Getting {instrument} data",
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
            try:
                if df.attrs["FULLNAME"] != instrument:
                    continue
                # Resample
                df = df.resample(resample_delta).max()
                assert (
                    df.shape[0] > 200
                ), f"Number of rows should be more than 200, got {df.shape[0]}"
                ## Path to save the image to
                # It's FOLDER / instrument / burst type / datetime_start.png
                path = os.path.join(
                    FOLDER,
                    instrument,
                    str(row["type"]),
                    row["datetime_start"].strftime("%Y-%m-%d_%H-%M-%S") + ".png",
                )
                os.makedirs(os.path.dirname(path), exist_ok=True)
                save_image(df.T, path)
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
        filtered_burst_list["datetime_start"].min(),
        filtered_burst_list["datetime_start"].max(),
    )
    print("Start Datetime:", min_datetime)
    print("End Datetime:", max_datetime)

    # Initialize tqdm with a large total and manually update
    desired_total_count = burst_generated * BURST_NON_BURST_RATIO
    pbar = tqdm(
        total=desired_total_count, desc=f"Getting non burst data for {instrument}"
    )
    while non_burst_generated < desired_total_count:
        start_datetime = return_random_datetime_between(min_datetime, max_datetime)
        # Now we need to check that the start_datetime is not in a burst
        non_burst_in_burst_df = burst_list[
            (burst_list.datetime_end <= start_datetime)
            & (start_datetime <= burst_list.datetime_end)
        ]
        if not non_burst_in_burst_df.empty:
            print("Datetime is in a burst, trying again...")
            continue
        end_datetime = start_datetime + timedelta(minutes=15)
        dfs = get_ecallisto_data(
            start_datetime,
            end_datetime,
            instrument_name=instrument,
            download_from_local=True,
        )
        for _, df in dfs.items():
            try:
                if instrument != df.attrs["FULLNAME"]:
                    continue
                # Resample
                df = df.resample(resample_delta).max()
                assert (
                    df.shape[0] > 200
                ), f"Number of rows should be more than 200, got {df.shape[0]}"
                # Maybe keep only good frequencies?
                # Background sub?
                ## Path to save the image to
                # It's FOLDER / instrument / burst type / start_datetime.png
                path = os.path.join(
                    FOLDER,
                    instrument,
                    "0",
                    start_datetime.strftime("%Y-%m-%d_%H-%M-%S") + ".png",
                )
                os.makedirs(os.path.dirname(path), exist_ok=True)
                save_image(df.T, path)
                pbar.update(1)  # Increment the progress bar by one
                non_burst_generated += 1
            except Exception as e:
                print(e)
                print(row["datetime_start"])
                print(row["datetime_end"])
                print(row["instruments"])
                print(instrument)

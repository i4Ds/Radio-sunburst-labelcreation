# %%
from ecallisto_ng.data_download.downloader import get_ecallisto_data
from ecallisto_ng.burst_list.utils import load_burst_list
from datetime import timedelta
from PIL import Image
import random
import os

FOLDER = "/mnt/nas05/data01/vincenzo/ecallisto/data"
RESOLUTION = (256, 256)
BURST_NON_BURST_RATIO = 5  # 5: There are 5x more non bust than burst images.
resample_delta = timedelta(minutes=15) / RESOLUTION[0]  # Ist nicht perfekt, aber geht
instruments = [
    "Australia-ASSA_02",
    "HUMAIN_59",
    "GERMANY-DLR_63",
    "SWISS-Landschlacht_62",
    "AUSTRIA-UNIGRAZ_01",
    "TRIEST_57",
    "NORWAY-EGERSUND_01",
    "SSRT_59",
    "INDIA-OOTY_02",
    "INDIA-GAURI_01",
    "ALASKA-COHOE_63",
    "USA-ARIZONA-ERAU_01",
    "MEXICO-LANCE-B_62",
    "MEXART_59",
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
    burst_list = burst_list[
        burst_list["instruments"].isin([instrument.split("_")[0]])
    ]  # Burstliste hat nur der Ort der Antenna, aber nicht die ID, darum #pythonmagic

    for i, row in burst_list.iterrows():
        datetime_start = row["datetime_start"] - random_duration(0, 11)
        end_time = datetime_start + timedelta(minutes=15)
        dfs = get_ecallisto_data(
            datetime_start, end_time, instrument_name=row["instruments"]
        )
        for _, df in dfs.items():
            try:
                if df.attrs["FULLNAME"] != instrument:
                    continue
                # Resample
                df = df.resample(resample_delta).max()
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
        burst_list["datetime_start"].min(),
        burst_list["datetime_start"].max(),
    )
    print("Start Datetime:", min_datetime)
    print("End Datetime:", max_datetime)

    while non_burst_generated < burst_generated * BURST_NON_BURST_RATIO:
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
            start_datetime, end_datetime, instrument_name=instrument
        )
        for _, df in dfs.items():
            try:
                if instrument != df.attrs["FULLNAME"]:
                    continue
                # Resample
                df = df.resample(resample_delta).max()
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
            except Exception as e:
                print(e)
                print(row["datetime_start"])
                print(row["datetime_end"])
                print(row["instruments"])
                print(instrument)

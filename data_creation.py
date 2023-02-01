# Download all spectograms with burst in the corresponding folder
import os
from tqdm import tqdm
from multiprocessing.pool import Pool as Pool
import datetime
import requests
from bs4 import BeautifulSoup
from tqdm import tqdm
from datetime import datetime, timedelta
import pandas as pd
import logging


LOCAL_DATA_FOLDER = os.path.join(os.path.abspath(os.sep), "var", "lib", "ecallisto")
FILES_BASE_URL = "http://soleil.i4ds.ch/solarradio/data/2002-20yy_Callisto/"
MIN_FILE_SIZE = 2000  # Minimum file size in bytes, to redownload empty files
LOG_FILE = os.path.join(
    os.path.abspath(os.sep),
    "var",
    "log",
    "ecallisto",
    f"log_data_creation_{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}.log",
)


def fetch_content(url):
    reqs = requests.get(url)
    soup = BeautifulSoup(reqs.text, "html.parser")
    logging.info(f"Function {fetch_content}")
    logging.info(f"Fetching content from {url}")
    logging.info(f"Status code: {reqs.status_code}")
    return soup


def extract_content(soup, substrings_to_include):
    """
    Extracts all the content from the given soup object based on the given parameters
    substrings_to_include: If specified, only links with the given substrings will be extracted
    substrings_to_exclude: If specified, links with the given substrings will be excluded

    Returns a list of all the links
    """
    content = []
    for link in soup.find_all("a"):
        if all(
            [pattern in link.get("href") for pattern in substrings_to_include]
        ):  # If none of the substrings are in the link
            content.append(link.get("href"))
    logging.info(f"Function {extract_content}")
    logging.info(
        f"Extracted {len(content)} files with the following substrings: {substrings_to_include}"
    )
    return content


def extract_fit_gz_files(url, instrument, substrings_to_include=None):
    """
    Extracts all the .fit.gz files from the given url
    instrument: If specified, only files with the instrument name will be extracted
    substrings_to_include: If specified, only files with the given substrings will be extracted

    Returns a list of all the .fit.gz files
    """
    soup = fetch_content(url)
    if substrings_to_include is None:
        substrings_to_include = [".fit.gz"]
    if instrument:
        substrings_to_include.append(instrument)

    logging.info(f"Function {extract_fit_gz_files}")
    logging.info(
        f"Extracting files with the following substrings: {substrings_to_include}"
    )
    return extract_content(soup, substrings_to_include=substrings_to_include)


def extract_fiz_gz_files_urls(year, month, day, instrument):
    """
    Extracts all the .fit.gz files from the given year, month and day
    instrument: If specified, only files with the instrument name will be extracted

    Returns a list of all the .fit.gz files
    """
    url = f"{FILES_BASE_URL}{year}/{month}/{day}/"
    file_names = extract_fit_gz_files(url, instrument=instrument)
    urls = [url + file_name for file_name in file_names]
    logging.info(f"Function {extract_fiz_gz_files_urls}")
    logging.info(f"Extracted {len(urls)} files")
    return urls


def download_ecallisto_file(URL, return_download_path=False, dir=LOCAL_DATA_FOLDER):
    # Split URL to get the file name and add the directory
    year, month, day, filename = URL.split("/")[-4:]
    directory = os.path.join(dir, year, month, day)
    os.makedirs(directory, exist_ok=True)

    # Check if the file already exists
    file_path = os.path.join(directory, filename)
    if (
        not os.path.exists(file_path) or os.path.getsize(file_path) < MIN_FILE_SIZE
    ):  # Check that it is not an empty file (e.g. 404 error)
        # Downloading the file by sending the request to the URL
        req = requests.get(URL)
        with open(file_path, "wb") as output_file:
            output_file.write(req.content)
    logging.info(f"Function {download_ecallisto_file}")
    logging.debug(f"Downloaded file {file_path}")
    # Return path (e.g. for astropy.io.fits.open)
    if return_download_path:
        return file_path


def download_ecallisto_files(
    dir,
    start_date=datetime.today().date() - timedelta(days=1),
    end_date=datetime.today().date(),
    instrument=None,
    return_download_paths=False,
    logger=None,
):
    """
    Downloads all the eCallisto files from the given start date to the end date. If the files already exist, they will not be downloaded again.
    start_date: Start date of the download (datetime object). If empty, it will be set to yesterday
    end_date: End date of the download (datetime object). If empty, it will be set to today
    instrument: If specified, only files with the instrument name will be extracted (substring).

    Returns None
    """
    assert (
        start_date <= end_date
    ), "Start date should be less than end date and both should be datetime objects"
    if logger is None:
        logging.basicConfig(
            filename=LOG_FILE,
            format="%(asctime)s %(levelname)-8s %(message)s",
            level=logging.DEBUG,
            datefmt="%Y-%m-%d %H:%M:%S",
            force=True,
        )
    if isinstance(instrument, str) and instrument.lower() in ["*", "all", ""]:
        instrument = None
    logging.info(
        f"Downloading files from {start_date} to {end_date} (instrument: {instrument if instrument else 'all'})"
    )
    paths = []
    for date in pd.date_range(start_date, end_date):
        day = date.day if date.day > 9 else f"0{date.day}"
        month = date.month if date.month > 9 else f"0{date.month}"
        urls = extract_fiz_gz_files_urls(date.year, month, day, instrument=instrument)
        for url in tqdm(urls, desc=f"Downloading {date}"):
            path = download_ecallisto_file(url, return_download_path=True, dir=dir)
            logging.debug(f"Downloaded file {path}")
            if return_download_paths:
                paths.append(path)

    if return_download_paths:
        return paths


if __name__ == "__main__":
    print(f"Downloading files to {LOCAL_DATA_FOLDER}")
    download_ecallisto_files(
        dir=LOCAL_DATA_FOLDER,
        start_date=datetime(2023, 1, 1),
        end_date=datetime(2023, 1, 2),
        instrument="ALASKA",
    )

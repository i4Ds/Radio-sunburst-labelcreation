import logging
import os
from datetime import datetime
from glob import glob

import numpy as np
import pandas as pd
from radiospectra.sources import CallistoSpectrogram

from database_functions import (
    add_new_column_sql,
    create_table_datetime_primary_key_sql,
    create_table_sql,
    get_column_names_sql,
    get_table_names_sql,
    insert_values_sql,
    table_to_hyper_table,
    to_float_if_possible,
)
from spectogram_utils import masked_spectogram_to_array, spec_time_to_pd_datetime

LOGGER = logging.getLogger("database_data_addition")


def get_column_names_clean(table_name):
    """Get the column names of a table in the database.

    Args:
        table_name (str): Name of the table in the database.

    Returns:
        list: List of column names without "" around the frequencies.
    """
    column_names = get_column_names_sql(table_name)
    column_names = [name.replace('"', "") for name in column_names]
    column_names = [to_float_if_possible(name) for name in column_names]
    return column_names


def extract_instrument_name(file_path):
    """Extract the instrument name from a file path.

    Parameters
    ----------
    file_path : str
        The file path to extract the instrument name from.

    Returns
    -------
    str
        The extracted instrument name, converted to lowercase with underscores in place of hyphens.

    Example
    -------
    >>> extract_instrument_name('/var/lib/ecallisto/2023/01/27/ALASKA-COHOE_20230127_001500_612.fit.gz')
    'alaska_cohoe_612'

    Notes
    -----
    The function first selects the last part of the file path and removes the extension.
    Then, it replaces hyphens with underscores and splits on underscores to get the parts of the file name.
    The function concatenates these parts, adding a numeric part of the file name if it is less than 6 digits.
    """
    # select last part of path and remove extension
    file_name_parts = os.path.basename(file_path).split(".")[0]
    # replace '-' with '_' and split on '_' to get the parts of the file name
    file_name_parts = file_name_parts.replace("-", "_").lower().split("_")
    file_name = ""
    for part in file_name_parts:
        if not part.isnumeric():
            file_name += "_" + part
    if (
        len(file_name_parts[-1]) < 6
    ):  # Sometimes, the last part is an ID number for when the station has multiple instruments.
        # We want to add this to the file name if it's not a time (6 digits).
        file_name = file_name + "_" + file_name_parts[-1]

    return file_name[1:]  # Remove the first '-'


def add_spec_from_path_to_database(path):
    """Add spectrogram data from a file to the database.

    Args:
        path (str): Path of the file containing the spectrogram data.
    """
    try:
        spec = CallistoSpectrogram.read(path)
    except Exception as e:
        LOGGER.error(f"Error: {e} for {os.path.basename(path)}")
        return
    spec = masked_spectogram_to_array(spec)
    instrument = extract_instrument_name(path)

    if not np.unique(spec.freq_axis).size == len(spec.freq_axis):
        LOGGER.warning(
            f"Warning: {os.path.basename(path)} has non-unique frequency axis"
        )
        spec = combine_non_unique_frequency_axis(spec)

    list_frequencies = number_list_to_postgresql_compatible_list(spec.freq_axis)
    list_frequencies.insert(0, "datetime")
    if not len(np.setdiff1d(list_frequencies, get_column_names_sql(instrument))) == 0:
        # LOGGER
        LOGGER.warning(f"Warning: {os.path.basename(path)} contains new columns")
        columns_to_add = np.setdiff1d(
            list_frequencies, get_column_names_sql(instrument)
        )
        LOGGER.debug(f"New columns: {columns_to_add}")
        for column in columns_to_add:
            add_new_column_sql(instrument, column, "SMALLINT")
        if len(get_column_names_sql(instrument)) > 1600:
            LOGGER.warning(
                f"Warning: File: {os.path.basename(path)} has above 1600 columns, which could cause performance issues."
            )

    sql_columns = ",".join(list_frequencies)
    data = np.array(spec.data, dtype=np.int16)
    assert np.all(data <= 32767)
    date_range = spec_time_to_pd_datetime(spec)
    sql_values = np_array_to_postgresql_array_with_datetime_index(date_range, data)
    insert_values_sql(instrument, sql_columns, sql_values)


def add_instrument_from_path_to_database(path):
    """Add an instrument to the database.

    Parameters
    ----------
    path : str
        The file path to the instrument data.

    Returns
    -------
    None

    Notes
    -----
    The function reads the instrument data using the `CallistoSpectrogram.read` function and converts it to an array using `masked_spectogram_to_array`.
    The instrument name is extracted using the `extract_instrument_name` function.
    If the frequency axis of the instrument data is not unique, a warning message is printed.
    The function then creates a table in the database with the extracted instrument name and columns corresponding to the frequency axis data, using `create_table_datetime_primary_key_sql` and `table_to_hyper_table`.
    """
    spec = CallistoSpectrogram.read(path)
    spec = masked_spectogram_to_array(spec)
    instrument = extract_instrument_name(path)
    if not np.unique(spec.freq_axis).size == len(spec.freq_axis):
        LOGGER.warning(
            f"Warning: {os.path.basename(path)} has non-unique frequency axis"
        )
        spec = combine_non_unique_frequency_axis(spec)
    sql_columns = numbers_list_to_postgresql_columns_meta_data(
        spec.freq_axis, "SMALLINT"
    )
    create_table_datetime_primary_key_sql(instrument, sql_columns, "datetime")
    table_to_hyper_table(instrument, "datetime")


def combine_non_unique_frequency_axis_mean(index, data):
    """Combine non-unique index data.

    Parameters
    ----------
    index : `~numpy.ndarray`
        The index data to combine the non-unique index data of.
    data : `~numpy.ndarray`
        The data to combine the non-unique index data of.

    Returns
    -------
    data : `~numpy.ndarray`
        The combined data.
    unique_idxs : `~numpy.ndarray`
        The unique index data.

    Notes
    -----
    The function first finds the unique index data and the indices of the non-unique index data.
    It then combines the non-unique index data using the method specified by the `method` parameter.
    """
    unique_idxs, indices = np.unique(index, return_inverse=True)
    data = np.array(
        [np.mean(data[indices == i], axis=0) for i in range(len(unique_idxs))]
    )
    return data, unique_idxs


def combine_non_unique_frequency_axis(spec, method="mean"):
    """Combine non-unique frequency axis data.

    Parameters
    ----------
    spec : `~radiospectra.spectrogram.Spectrogram`
        The spectrogram to combine the frequency axis data of.
    method : str, optional
        The method to use to combine the frequency axis data. Defaults to "mean".

    Returns
    -------
    `~radiospectra.spectrogram.Spectrogram`
        The spectrogram with combined frequency axis data.

    Notes
    -----
    The function first finds the unique frequency axis data and the indices of the non-unique frequency axis data.
    It then combines the non-unique frequency axis data using the method specified by the `method` parameter.
    """
    if method == "mean":
        data, unique_freq_axis = combine_non_unique_frequency_axis_mean(
            spec.freq_axis, spec.data
        )
    else:
        raise ValueError(f"Method {method} not supported")
    spec.data = data
    spec.freq_axis = unique_freq_axis

    return spec


def glob_files_for_date(dir_path, date, file_name_pattern, extension):
    """Glob all files for a given date.

    Args:
        dir_path (str): Path to the directory containing the files.
        date (datetime.date): Date for which to glob the files.
        file_name_pattern (str): Pattern of the file name.
        extension (str): Extension of the files.

    Returns:
        list: List of file paths.
    """
    file_path_pattern = os.path.join(
        dir_path, str(date.year), str(date.month).zfill(2), str(date.day).zfill(2)
    )
    path_to_glob = os.path.join(file_path_pattern, f"{file_name_pattern}.{extension}")
    return glob(path_to_glob, recursive=True)


def glob_files(
    dir_path, start_date, end_date, file_name_pattern="*", extension="fit.gz"
):
    """Retrieve file paths in a directory for a given date range.

    Parameters
    ----------
    dir_path : str
        The path to the directory.
    start_date : str
        The start date in the format "YYYY-MM-DD".
    end_date : str
        The end date in the format "YYYY-MM-DD".
    file_name_pattern : str, optional
        The file name pattern to match. Default is "*".
    extension : str, optional
        The file extension to match. Default is "fit.gz".

    Returns
    -------
    list of str
        The list of file paths.

    Notes
    -----
    The function generates a date range between `start_date` and `end_date` using `pd.date_range`, and retrieves file paths for each date using `glob_files_for_date`.
    The function concatenates the file paths for each date and returns the result.
    """
    file_paths = []
    for date in pd.date_range(start_date, end_date):
        file_paths += glob_files_for_date(dir_path, date, file_name_pattern, extension)

    return file_paths


def extract_constant_meta_data(specs, name):
    """
    Extract the constant metadata from the header of multiple spectrograms.

    Parameters:
        specs (list): List of spectrogram instances to extract metadata from.
        name (str): Name of the instrument.

    Returns:
        dict: A dictionary of metadata that is constant between the spectrogram instances.

    """
    meta_data = {}
    for spec in specs:
        for key, value in dict(spec.header).items():
            key = key.lower()
            if isinstance(value, str):
                value = value.strip()
            if key not in meta_data and value is not None and value != "":
                meta_data[key] = value
            else:
                if meta_data[key] != value:
                    meta_data[key] = None

    # Drop the keys that are None
    meta_data = {key: value for key, value in meta_data.items() if value is not None}
    meta_data["instrument"] = name
    return meta_data


def extract_separate_instruments(paths):
    """Extracts the unique instrument names from a list of file paths."""
    instruments = []
    for path in paths:
        instrument = extract_instrument_name(path)
        if instrument not in instruments:
            instruments.append(instrument)
    return instruments


def create_dict_of_instrument_paths(paths):
    """Creates a dictionary of instrument names and their corresponding file paths."""
    instruments = extract_separate_instruments(paths)
    instrument_paths = {}
    for instrument in instruments:
        instrument_paths[instrument] = []
    for path in paths:
        instrument = extract_instrument_name(path)
        instrument_paths[instrument].append(path)
    return instrument_paths


def is_table_in_db(table_name):
    return table_name in get_table_names_sql()


def number_list_to_postgresql_compatible_list(names):
    """
    Converts a list to a string. Because postgresql does not allow
    numbers as column names, the numbers are encapsulated in "".
    If types is a list, it is assumed that the types correspond to the names. Otherwise, the same type is used for all names.
    """
    names_ = []
    for name in names:
        if isinstance(name, int) or isinstance(name, float) or name[0].isnumeric():
            names_.append(f'"{name}"')
        else:
            names_.append(name)

    return names_.copy()


def numbers_list_to_postgresql_columns_meta_data(names, types):
    """
    Adds the types to the names.
    """
    names = number_list_to_postgresql_compatible_list(names)
    if isinstance(types, list):
        return ", ".join([f"{name} {type}" for (name, type) in zip(names, types)])
    else:
        return ", ".join([f"{name} {types}" for name in names])


def create_table(instrument_name, frequencies_columns, types="REAL"):
    """
    Creates a table for the given instrument with the given frequencies.
    """
    columns_meta_data = numbers_list_to_postgresql_columns_meta_data(
        frequencies_columns, types=types
    )
    create_table_sql(instrument_name, columns_meta_data)


def np_array_to_postgresql_array(array):
    """
    Converts a numpy array to a string that can be used in a postgresql array.

    Args:
        array (numpy.ndarray): the input numpy array

    Returns:
        str: a string representation of the numpy array in a format that can be used in a postgresql array
    """
    # Convert the array to a list of tuples
    list_of_tuples = [tuple(row) for row in array]

    # Use the str function to format each tuple as a string
    formatted_list = [str(tuple(row)) for row in list_of_tuples]

    # Join the strings with a comma to create the final format
    final_format = ",".join(formatted_list)
    return final_format


def np_array_to_postgresql_array_with_datetime_index(index, array):
    """
    Converts a numpy array and a datetime index to a string that can be used in a postgresql array.

    Args:
        index (list): a list of datetime objects representing the index of the input numpy array
        array (numpy.ndarray): the input numpy array

    Returns:
        str: a string representation of the numpy array and datetime index in a format that can be used in a postgresql array
    """
    # Convert the datetime index to strings in a format that can be used in a postgresql array
    index_strings = [val.strftime("%Y-%m-%d %H:%M:%S.%f") for val in index]

    # Convert the numpy array to a list of tuples
    list_of_tuples = [tuple(row) for row in zip(index_strings, *array.tolist())]

    # Use the str function to format each tuple as a string
    formatted_list = [str(tup) for tup in list_of_tuples]

    # Join the strings with a comma to create the final format
    final_format = ",".join(formatted_list)
    return final_format


def fill_missing_timesteps_with_nan(df):
    """
    Fill missing timesteps in a pandas DataFrame with NaN values.

    Parameters
    ----------
    df : pandas.DataFrame
        The DataFrame to fill missing timesteps in.

    Returns
    -------
    pandas.DataFrame
        A new DataFrame with missing timesteps filled with NaN values.

    Notes
    -----
    This function is useful when working with time-series data that has missing timesteps.
    By filling the missing timesteps with NaN values, the DataFrame can be easily visualized
    or analyzed without introducing errors due to missing data.

    The function calculates the median time delta of the input DataFrame, and then creates
    a new index with evenly spaced values based on that delta. It then uses the pandas
    `reindex` function to fill in missing timesteps with NaN values.

    Examples
    --------
    >>> dates = pd.date_range('2023-02-19 01:00', '2023-02-19 05:00', freq='2H')
    >>> freqs = ['10M', '20M', '30M']
    >>> data = np.random.randn(len(dates), len(freqs))
    >>> df = pd.DataFrame(data, index=dates, columns=freqs)
    >>> df = fill_missing_timesteps_with_nan(df)
    >>> print(df)

                            10M       20M       30M
    2023-02-19 01:00:00 -0.349636  0.004947  0.546848
    2023-02-19 03:00:00       NaN       NaN       NaN
    2023-02-19 05:00:00 -0.576182  1.222293 -0.416526
    """
    time_delta = np.median(np.diff(df.index.values))
    time_delta = pd.Timedelta(time_delta)
    new_index = pd.date_range(df.index[0], df.index[-1], freq=time_delta)
    df = df.reindex(new_index)
    return df

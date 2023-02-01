import os
from glob import glob
from database_functions import get_table_names_sql, create_table_sql
from datetime import datetime


def extract_instrument_name(file_path):
    """Extracts the instrument name from a file path.
    Example: /var/lib/ecallisto/2023/01/27/ALASKA-COHOE_20230127_001500_612.fit.gz -> alaska-cohoe_612"""
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


def glob_files(dir_path, file_path_pattern, file_name_pattern, extension):
    path_to_glob = os.path.join(
        dir_path, file_path_pattern, f"{file_name_pattern}.{extension}"
    )
    return glob(path_to_glob, recursive=True)


def extract_constant_meta_data(specs, name):
    """Extract the meta data from the spectrograms that does not change between spectrograms.

    Args:
        specs (list): List of spectrograms to extract the meta data from the header which does not change between spectrograms
        name (name of the instrument): Name of the instrument
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


def python_type_to_postgresql_type(python_type):
    if python_type == int:
        return "INTEGER"
    elif python_type == float:
        return "REAL"
    elif python_type == str:
        return "TEXT"
    elif python_type == datetime:
        return "TIMESTAMP"
    elif python_type == bool:
        return "BOOLEAN"
    else:
        raise ValueError(f"Unknown python type: {python_type}.")


def np_array_to_postgresql_array(array):
    """
    Converts a numpy array to a string that can be used in a postgresql array.
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

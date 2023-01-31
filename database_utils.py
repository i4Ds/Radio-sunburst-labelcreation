import os
from glob import glob
from database_functions import get_table_names_sql, create_table_sql


def extract_instrument_name(file_path):
    """Extracts the instrument name from a file path.
    Example: /var/lib/ecallisto/2023/01/27/ALASKA-COHOE_20230127_001500_612.fit.gz -> alaska-cohoe_612"""
    file_name_parts = os.path.basename(file_path).split(".")[0].lower().split("_")
    file_name = ""
    for part in file_name_parts:
        if not part.isnumeric():
            file_name += "-" + part
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
            if key not in meta_data:
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


def numbers_list_to_postgresql_columns_meta_data(list_, types):
    """
    Converts a list to a string. Because postgresql does not allow
    numbers as column names, the numbers are encapsulated in "".
    """
    if isinstance(types, list):
        return ", ".join([f'"{item}" {type}' for (item, type) in zip(list_, types)])
    else:
        return ", ".join([f'"{item}" {types}' for item in list_])


def create_table(instrument_name, frequencies_columns, type="REAL"):
    """
    Creates a table for the given instrument with the given frequencies.
    """
    columns_meta_data = numbers_list_to_postgresql_columns_meta_data(
        frequencies_columns, type=type
    )
    create_table_sql(instrument_name, columns_meta_data)

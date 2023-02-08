from database_functions import *
from database_utils import *
import argparse
from data_creation import LOCAL_DATA_FOLDER
from datetime import datetime, timedelta
from tqdm import tqdm
from multiprocessing.pool import Pool as Pool

LOG_FILE = os.path.join(
    os.path.abspath(os.sep),
    "var",
    "log",
    "ecallisto",
    f"log_data_addition_to_datebase_main_{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}.log",
)

def main(start_date, end_date, instrument, dir):
    paths = glob_files(dir, start_date, end_date, instrument)
    dict_paths = create_dict_of_instrument_paths(paths)
    
    t = tqdm(dict_paths.keys(), desc="Adding instruments to database", position=0)
    for instrument in t:
        if instrument not in get_table_names_sql():
            # Get random file path to get meta data and create table
            file_path = dict_paths[instrument][0]
            add_instrument_from_path_to_database(file_path)
        with Pool() as p:
            r = list(tqdm(p.imap(add_spec_from_path_to_database, dict_paths[instrument]), total=len(dict_paths[instrument]), desc="Adding specs to database", position=1))

if __name__ == "__main__":
    logging.basicConfig(
        filename=LOG_FILE,
        format="%(asctime)s %(levelname)-8s %(message)s",
        level=logging.DEBUG,
        datefmt="%Y-%m-%d %H:%M:%S",
        force=True,
    )
    ## Example:
    # python load_data_into_database.py --start_date 2020-01-01 --end_date 2020-01-02 --instrument all
    # Get arguments from command line
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--start_date",
        type=str,
        default=(datetime.today().date() - timedelta(days=14)))
    parser.add_argument(
        "--end_date",
        type=str,
        default=datetime.today().date())
    parser.add_argument(
        "--instrument",
        type=str,
        default="*")
    parser.add_argument(
        "--dir",
        type=str,
        default=LOCAL_DATA_FOLDER)
    args = parser.parse_args()
    logging.info(f"Arguments: {args}")
    # Update date to datetime
    args.start_date = datetime.strptime(args.start_date, "%Y-%m-%d").date()
    args.end_date = datetime.strptime(args.end_date, "%Y-%m-%d").date()
    try:
        # Main
        main(**vars(args))
    except Exception as e:
        logging.exception(e)
        raise e
    
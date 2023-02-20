import os

import pandas as pd
import psycopg2

# Create variables for the connection to the OS
os.environ["PGHOST"] = "localhost"
os.environ["PGUSER"] = "postgres"
os.environ["PGPASSWORD"] = "1234"

##
CONNECTION = f' dbname=tsdb user={os.environ["PGUSER"]} host={os.environ["PGHOST"]} password={os.environ["PGPASSWORD"]}'


def create_table_sql(table_name, columns):
    """
    Creates a table with the given name and columns
    """
    with psycopg2.connect(CONNECTION) as conn:
        cursor = conn.cursor()
        cursor.execute(
            f"""CREATE TABLE {table_name} (
                            id SERIAL PRIMARY KEY,
                            {columns}
                        );
                        """
        )
        conn.commit()
        cursor.close()


def table_to_hyper_table(instrument, datetime_column):
    """
    Creates a table in the hyper database with the same name and columns as the given table.
    """
    with psycopg2.connect(CONNECTION) as conn:
        cursor = conn.cursor()
        cursor.execute(
            f"""SELECT create_hypertable('{instrument}', '{datetime_column}'
                        );
                        """
        )


def create_table_datetime_primary_key_sql(table_name, columns, datetime_column):
    """
    Creates a table with the given name and columns
    """
    with psycopg2.connect(CONNECTION) as conn:
        cursor = conn.cursor()
        cursor.execute(
            f"""CREATE TABLE {table_name} (
                            {datetime_column} TIMESTAMP PRIMARY KEY,
                            {columns}
                        );
                        """
        )
        conn.commit()
        cursor.close()


def drop_table_sql(table_name):
    """
    Drops a table from the database if it exists
    """
    with psycopg2.connect(CONNECTION) as conn:
        cursor = conn.cursor()
        cursor.execute(
            f"""DROP TABLE IF EXISTS {table_name};
                        """
        )
        conn.commit()
        cursor.close()


def get_table_names_sql():
    with psycopg2.connect(CONNECTION) as conn:
        cursor = conn.cursor()
        cursor.execute(
            """SELECT table_name
                       FROM information_schema.tables
                       WHERE table_schema='public'
                       AND table_type='BASE TABLE';
                       """
        )

        tuple_list = cursor.fetchall()
        return [tup[0] for tup in tuple_list]


def add_new_column_sql(table_name, column_name, column_type):
    """
    Adds a new column to the given table
    """
    with psycopg2.connect(CONNECTION) as conn:
        cursor = conn.cursor()
        cursor.execute(
            f"""ALTER TABLE {table_name}
                        ADD COLUMN IF NOT EXISTS {column_name} {column_type};
                        """
        )
        conn.commit()
        cursor.close()


def get_column_names_sql(table_name):
    with psycopg2.connect(CONNECTION) as conn:
        cursor = conn.cursor()
        cursor.execute(
            f"""SELECT column_name
                       FROM information_schema.columns
                       WHERE table_name = '{table_name}';
                       """
        )

        tuple_list = cursor.fetchall()
        tuple_list = [tup[0] for tup in tuple_list]
        tuple_list = sort_column_names(tuple_list)
        tuple_list = [
            f'"{tup}"' if "datetime" not in tup else tup for tup in tuple_list
        ]
        return tuple_list


def timebucket_values_from_database_sql(
    table, start_time, end_time, columns=None, timebucket="1H", agg_function="avg"
):
    """
    Returns all values between start and end time in the given table, timebucketed and aggregated.
    """
    if not columns:
        columns = get_column_names_sql(table)
        columns = [column for column in columns if column != "datetime"]
    agg_function_sql = ",".join(
        [f"{agg_function}({column}) AS {column}" for column in columns]
    )
    query = f"SELECT time_bucket('{timebucket}', datetime) AS time, {agg_function_sql} FROM {table} WHERE datetime BETWEEN '{start_time}' AND '{end_time}' GROUP BY time ORDER BY time"

    with psycopg2.connect(CONNECTION) as conn:
        with conn.cursor() as cur:
            query = f"SELECT time_bucket('{timebucket}', datetime) AS time, {agg_function_sql} FROM {table} WHERE datetime BETWEEN '{start_time}' AND '{end_time}' GROUP BY time ORDER BY time"
            cur.execute(query)
            return cur.fetchall()


def insert_values_sql(table_name, columns, values):
    """
    Inserts values into the given table. If they already exist, the value is skipped.
    """
    with psycopg2.connect(CONNECTION) as conn:
        cursor = conn.cursor()
        cursor.execute(
            f"""INSERT INTO {table_name} ({columns})
                        VALUES {values}
                        ON CONFLICT DO NOTHING;
                        """
        )
        conn.commit()
        cursor.close()


def get_size_of_table(table_name):
    """
    Returns the size of the given table in MB
    """
    with psycopg2.connect(CONNECTION) as conn:
        cursor = conn.cursor()
        cursor.execute(
            f"""SELECT pg_size_pretty(pg_total_relation_size('{table_name}'));
                        """
        )
        size = cursor.fetchone()[0]
        return size


def get_size_of_database():
    """
    Returns the size of the database in MB
    """
    with psycopg2.connect(CONNECTION) as conn:
        cursor = conn.cursor()
        cursor.execute(
            f"""SELECT pg_size_pretty(pg_database_size('tsdb'));
                        """
        )
        size = cursor.fetchone()[0]
        return size


def get_values_from_database_sql(table, start_time, end_time, columns=None):
    """
    Returns the values from the given table between the given start and end time
    """
    if columns is None:
        columns = "*"

    with psycopg2.connect(CONNECTION) as conn:
        with conn.cursor() as cur:
            cur.execute(
                f"SELECT {columns} FROM {table} WHERE datetime BETWEEN '{start_time}' AND '{end_time}'"
            )
            return cur.fetchall()


def sql_result_to_df(result, columns, meta_data: dict = None):
    """
    Converts the given result from a sql query to a pandas dataframe
    """
    df = pd.DataFrame(result, columns=columns)
    df["datetime"] = pd.to_datetime(df["datetime"])
    df = df.set_index("datetime")
    if meta_data:
        for key, value in meta_data.items():
            df.attrs[key] = value
    return df


def sort_column_names(list):
    return sorted(list, key=lambda x: to_float_if_possible_else_number(x, -1000))


def is_float(element) -> bool:
    # If you expect None to be passed:
    if element is None:
        return False
    try:
        float(element)
        return True
    except ValueError:
        return False


def to_float_if_possible(element):
    if is_float(element):
        return float(element)
    else:
        return element


def to_float_if_possible_else_number(element, number):
    if is_float(element):
        return float(element)
    else:
        return number

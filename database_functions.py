import psycopg2
import os

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

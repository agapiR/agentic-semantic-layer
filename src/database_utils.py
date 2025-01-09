import os
import re
import json
import random
import sqlite3
import snowflake.connector
import sqlalchemy
import sqlalchemy_schemadisplay
import networkx as nx


def create_local_database(database_dir, ddl, verbose=True, replace=False):
    """
    Create a new SQLite database with the given a Data Definition Language (DDL) script.
    """
    # Remove the existing database if it exists
    if replace:
        try:
            os.remove(database_dir)
        except OSError:
            pass
    else:
        if os.path.exists(database_dir):
            raise Exception(f"Database {database_dir} already exists. Set replace=True to overwrite.")

    # Connect to the database
    conn = sqlite3.connect(database_dir)
    cursor = conn.cursor()

    # Split the script into individual queries
    queries = ddl.split(";")
    for query in queries:
        if query.strip() == "":
            continue
        if verbose:
            print(f"Executing query: {query}")
        cursor.execute(query)
        conn.commit()

    # Close the connection
    conn.close()

    return

def create_snowflake_database(database_name, snowflake_config_file, ddl, verbose=True):
    """
    Create a new Snowflake database with the given a Data Definition Language (DDL) script.
    """
    # Load the Snowflake configuration
    try:
        snowflake_config = json.load(open(snowflake_config_file))[database_name]
    except KeyError:
        raise KeyError(f"Database {database_name} not found in the config file.")

    # Connect to Snowflake
    ctx = snowflake.connector.connect(
        user=snowflake_config['user'],
        password=snowflake_config['password'],
        account=snowflake_config['account'],
        role=snowflake_config['role'],
    )

    # Connect to the clone database
    cs = ctx.cursor()

    # Set current warehouse, database and schema
    cs.execute(f"USE WAREHOUSE {snowflake_config['warehouse']}")
    cs.execute(f"CREATE DATABASE IF NOT EXISTS {snowflake_config['database']}")
    cs.execute(f"USE DATABASE {snowflake_config['database']}")
    cs.execute(f"CREATE SCHEMA IF NOT EXISTS {snowflake_config['schema']}")
    cs.execute(f"USE SCHEMA {snowflake_config['schema']}")

    # Split the script into individual queries
    queries = ddl.split(";")
    for query in queries:
        if query.strip() == "":
            continue
        query = query.replace("CREATE", "CREATE OR REPLACE")
        if verbose:
            print(f"Executing query: {query}")
        cs.execute(query)

    # Close the connection
    cs.close()
    ctx.close()

    return


def schema_image(database_dir, image_name):
    """
    Generate an image of the schema of the SQLite database.
    """
    # Create a connection to the database
    engine = sqlalchemy.create_engine(f"sqlite:///{database_dir}", )
    metadata = sqlalchemy.MetaData()
    metadata.reflect(bind=engine, views=True)

    # Generate the schema image
    img = sqlalchemy_schemadisplay.create_schema_graph(
            engine,
            metadata=metadata,
            show_datatypes=True,
            show_indexes=False,
            rankdir='LR',
            concentrate=False
        )
    img.write_png(image_name)

    engine.dispose()

    return img


def copy_local_database(database_source_dir, database_updated_dir, verbose=True, replace=False):
    """
    Copy the schema and data from the original SQLite database to a new SQLite database.
    Input:
    - database_source_dir: the path to the original database
    - database_updated_dir: the path to the new database
    - verbose: whether to print progress messages
    """
    # Remove the existing database if it exists
    if replace:
        try:
            os.remove(database_updated_dir)
        except OSError:
            pass
    else:
        if os.path.exists(database_updated_dir):
            raise Exception(f"Database {database_updated_dir} already exists. Set replace=True to overwrite.")
    
    # Connect to the original database
    original_conn = sqlite3.connect(database_source_dir)

    # Connect to the new database
    if os.path.exists(database_updated_dir):
        os.remove(database_updated_dir)
    new_conn = sqlite3.connect(database_updated_dir)

    # Get the cursor for both databases
    new_cursor = new_conn.cursor()

    # Extract the schema and data from the original database
    schema_data = ""
    for line in original_conn.iterdump():
        schema_data += f"{line}\n"

    # Execute the schema and data in the new database
    if verbose:
        print("Copying the original database...")
    new_cursor.executescript(schema_data)

    # Commit the changes and close the connections
    new_conn.commit()
    new_conn.close()
    original_conn.close()

    return


def get_view_name_from_definition(view_definition):
    view_definition = view_definition.lower()
    if re.findall(r'create view if not exists (.*?) as', view_definition):
        view_name = re.findall(r'create view if not exists (.*?) as', view_definition)
        return view_name[0].strip()
    elif re.findall(r'create or replace view (.*?) as', view_definition):
        view_name = re.findall(r'create or replace view (.*?) as', view_definition)
        return view_name[0].strip()
    elif re.findall(r'create view (.*?) as', view_definition):
        view_name = re.findall(r'create view (.*?) as', view_definition)
        return view_name[0].strip()
    else:
        return None
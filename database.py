import os
import re
import json
import random
import sqlite3
import sqlalchemy
import sqlalchemy_schemadisplay
import snowflake.connector
import networkx as nx
from database_utils import get_view_name_from_definition

################################
# TODO: Implement a Database class that can be used to interact with the database
# The class should have the following methods:
# - schema_dictionary: Get the schema of the database in a dictionary format,
#                       where keys are table names and values are lists of column names
# - schema_wording: Get the schema of the database in a textualized format.
#                       The schema includes the table names, column names, column types, primary keys, and foreign keys.
#                       If include_sample_data is set to True, the schema will also include sample data from the tables.
# - schema_graph: Generate a networkx graph object representing the schema of the database. 
#                     (every node represents a table and has id and description / label)
#                     (every edge represents a foreign key relationship between two tables, has source and target ids and description / label)
# - materialize_view: Materialize a view in the database.
#                     The method should take the view definition as input and return a message indicating the success or failure of the operation.
#                     The method should also have an option to persist the materialized view after the operation is complete.
# Optional methods:
# - schema_image: Generate an image of the schema of the database.
################################


class Database:
    def __init__(self, database_name):
        self.db_name = database_name

    @property
    def database_name(self):
        return self.db_name
    
    def get_tables(self):
        """
        Get a list of table names in the database.
        """
        raise NotImplementedError
    
    def get_columns_of_table(self, table_name):
        """
        Get a list of column names for a given table.
        """
        raise NotImplementedError
    
    def schema_dictionary(self, include_views=False):
        """
        Get schema of the database in dictionary format.
        Table name is the key and the value is a list of column names.
        """
        raise NotImplementedError
    
    def schema_wording(self, selected_tables=None, include_sample_data=True, sample_size=5):
        """
        Generate a textual description of the schema of the database.
        """
        raise NotImplementedError

    def schema_graph(self, save_dir=None):
        """
        Generate a networkx graph object representing the schema of the database.
        """
        raise NotImplementedError
    
    def materialize_view(self, view_definition, verbose=True, persist=False):
        """
        Materialize a view in the database.
        """
        raise NotImplementedError

"""
SQLite Database class
"""
class SQLiteDatabase(Database):
    def __init__(self, database_name, database_dir):
        super().__init__(database_name)
        self.db_dir = database_dir

    def get_tables(self):
        """
        Get a list of table names in the SQLite database.
        """
        conn = sqlite3.connect(self.db_dir)
        cursor = conn.cursor()
        cursor.execute("SELECT name FROM sqlite_master WHERE type = 'table'")
        tables = [table[0] for table in cursor.fetchall()]
        conn.close()
        return tables
    
    def get_columns_of_table(self, table_name):
        """
        Get a list of column names for a given table in the SQLite database.
        """
        conn = sqlite3.connect(self.db_dir)
        cursor = conn.cursor()
        cursor.execute(f"PRAGMA table_info({table_name})")
        columns = [column[1] for column in cursor.fetchall()]
        conn.close()
        return columns
    
    def schema_dictionary(self, include_views=False):
        """
        Get schema of the SQLite database in dictionary format. Table (and view) name is the key and the list of column names is the value.
        include_views: If True, include view names as keys in the schema dictionary.
        """
        schema = {}
        conn = sqlite3.connect(self.db_dir)
        cursor = conn.cursor()

        # fetch table names
        cursor.execute("SELECT name FROM sqlite_master WHERE type = 'table'")
        tables = [str(table[0].lower()) for table in cursor.fetchall()]

        # fetch view names
        if include_views:
            cursor.execute("SELECT name FROM sqlite_master WHERE type = 'view'")
            views = [str(view[0].lower()) for view in cursor.fetchall()]
            tables.extend(views)

        # fetch table info
        for table in tables:
            try:
                cursor.execute("PRAGMA table_info({})".format(table))
                schema[table] = [str(col[1].lower()) for col in cursor.fetchall()]
            except Exception as e:
                # print(f"Error in table {table}. {e}")
                pass
        
        # Close the connection
        conn.close()

        return schema
    
    def schema_wording(self, selected_tables=None, include_sample_data=True, sample_size=5):
        """
        Generate a textual description of the schema of the SQLite database, in the form of Data Definition Language (DDL) statements.
        """
        # Connect to the SQLite database
        conn = sqlite3.connect(self.db_dir)
        cursor = conn.cursor()

        # Query to get the tables
        tables = self.get_tables()

        # Readout the schema
        DDL = ''
        for table_id, table_name in enumerate(tables):
            if selected_tables and table_name not in selected_tables:
                continue
            DDL += f"CREATE TABLE {table_name} (\n"

            cursor.execute(f"PRAGMA table_info({table_name})")
            columns = cursor.fetchall()

            # Column details
            for column_id, column in enumerate(columns):
                cid, name, type_, notnull, dflt_value, pk = column
                DDL += f"  {name} {type_}"
                if notnull == 1:
                    DDL += " NOT NULL"
                if dflt_value:
                    DDL += f" DEFAULT {dflt_value}"
                if pk == 1:
                    DDL += " PRIMARY KEY"
                if column_id < len(columns) - 1:
                    DDL += ","
                DDL += "\n"

            # Foreign key details
            cursor.execute(f"PRAGMA foreign_key_list({table_name})")
            fks = cursor.fetchall()
            for fk in fks:
                fk_id, fk_seq, fk_table, fk_from, fk_to, fk_on_update, fk_on_delete, fk_match = fk
                DDL += f"  FOREIGN KEY ({fk_from}) REFERENCES {fk_table}({fk_to})\n"

            DDL += ");\n\n"

            # Sample data
            if include_sample_data:
                cursor.execute(f"SELECT * FROM {table_name}")
                # Handle the case where the table is empty
                if not cursor.rowcount:
                    DDL += f"-- Sample Data: No sample data available\n\n"
                else:
                    data = cursor.fetchall()
                    DDL += f"-- Sample Data:\n"
                    for row in data[:sample_size]:
                        DDL += f"{row}\n"

            DDL += "\n"

        # Close the connection
        conn.close()

        return DDL

    def schema_wording_simple(self, selected_tables=None, include_sample_data=True, sample_size=5):
        """
        Generate a textual description of the schema of the SQLite database.
        """
        # Connect to the SQLite database
        conn = sqlite3.connect(self.db_dir)
        cursor = conn.cursor()

        # Query to get the tables
        tables = self.get_tables()

        # Readout the schema
        schema = ''
        for table_id, table_name in enumerate(tables):
            if selected_tables and table_name not in selected_tables:
                continue
            schema += f"Table: {table_name}\n"
            schema += "=" * (7 + len(table_name)) + "\n"

            cursor.execute(f"PRAGMA table_info({table_name})")
            columns = cursor.fetchall()

            # Column details
            for column in columns:
                cid, name, type_, notnull, dflt_value, pk = column
                schema += f"Column: {name}\n"
                schema += f"  Type: {type_}\n"
                if pk == 1:
                    schema += f"  Primary Key\n"

            # Foreign key details
            cursor.execute(f"PRAGMA foreign_key_list({table_name})")
            fks = cursor.fetchall()
            for fk in fks:
                fk_id, fk_seq, fk_table, fk_from, fk_to, fk_on_update, fk_on_delete, fk_match = fk
                schema += f"Foreign key {fk_from} references the primary key {fk_to} of table {fk_table}\n"

            # Sample data
            if include_sample_data:
                cursor.execute(f"SELECT * FROM {table_name}")
                # Handle the case where the table is empty
                if not cursor.rowcount:
                    schema += "Sample Data: No sample data available\n"
                else:
                    data = cursor.fetchall()
                    sample_data = random.sample(data, min(sample_size, len(data)))
                    schema += "Sample Data:\n"
                    for row in sample_data:
                        schema += f"  {row}\n"
            
            schema += "\n"

        # Close the connection
        conn.close()

        return schema
    
    def schema_graph(self):
        """
        Generate a networkx graph object representing the schema of the SQLite database.
        """
        # Connect to the SQLite database
        conn = sqlite3.connect(self.db_dir)
        cursor = conn.cursor()

        # Query to get the tables
        tables = self.get_tables()

        # Query to get the foreign-primary key pairs
        fk_pk_pairs = []
        for table_id, table_name in enumerate(tables):
            cursor.execute(f"PRAGMA foreign_key_list({table_name})")
            fks = cursor.fetchall()
            for fk in fks:
                fk_id, fk_seq, fk_table, fk_from, fk_to, fk_on_update, fk_on_delete, fk_match = fk
                fk_table_id = tables.index(fk_table)
                fk_pk_pairs.append((table_id, table_name, fk_table_id, fk_table, fk_from, fk_to))
        
        # Close the connection
        conn.close()
        
        # Make the schema graph 
        G = nx.DiGraph()

        # Create nodes representing tables
        for table_id, table_name in enumerate(tables):
            G.add_node(table_id, name=table_name)

        # Create edges representing foreign-primary key pairs
        for table_id, _, fk_table_id, _, _, _ in fk_pk_pairs:
            G.add_edge(table_id, fk_table_id)
            G.add_edge(fk_table_id, table_id)
        
        return G
    
    def materialize_view(self, view_definition, verbose=True, persist=False):
        """
        Materialize a view in the SQLite database.
        """
        # Connect to the database
        conn = sqlite3.connect(self.db_dir)
        cursor = conn.cursor()
        
        # Get the view name 
        view_name = get_view_name_from_definition(view_definition)

        # Materialize the view
        if verbose:
            print("Materializing the view...")    
        done = False
        while not done:
            try:
                cursor.execute(view_definition)
                cursor.execute(f"SELECT * FROM {view_name}") # Because SQLite uses Deferred Syntax Checking...
                conn.commit()
                done = True
            except Exception as e:
                # If the error says the view already exists, drop the view and try again
                if "already exists" in str(e):
                    if verbose:
                        print(f"View {view_name} already exists. Dropping the view and running the query again...")
                    cursor.execute(f"DROP VIEW {view_name}")
                    conn.commit()
                # Else return the error
                else:
                    if verbose:
                        print(f"Error in creating view {view_name}. Error received:\n{e}.")
                    cursor.execute(f"DROP VIEW {view_name}") # Drop if an error occurs, SQLite still retains the view definition...
                    conn.commit()
                    conn.close()
                    return f"Error in creating view {view_name}. Error received:\n{e}."

        # Drop the view if not persisting
        if not persist:
            cursor.execute(f"DROP VIEW {view_name}")
        
        if verbose:
            if persist:
                print(f"View {view_name} was defined successfully.")
            else:
                print(f"View {view_name} was defined successfully. The view has been dropped.")

        # Commit the changes and close the connection
        conn.commit()
        conn.close()

        return f"View {view_name} successfully defined."


"""
Snowflake Database class
"""
class SnowflakeDatabase(Database):
    def __init__(self, database_name, snowflake_config_file):
        super().__init__(database_name)
        try:
            self.snowflake_config = json.load(open(snowflake_config_file))[database_name]
        except KeyError:
            raise KeyError(f"Database {database_name} not found in the config file.")
        assert 'account' in self.snowflake_config, "Snowflake account not found in the config file."
        assert 'user' in self.snowflake_config, "Snowflake user not found in the config file."
        assert 'password' in self.snowflake_config, "Snowflake password not found in the config file."
        assert 'role' in self.snowflake_config, "Snowflake role not found in the config file."
        assert 'warehouse' in self.snowflake_config, "Snowflake warehouse not found in the config file."
        assert 'database' in self.snowflake_config, "Snowflake database not found in the config file."
        assert 'schema' in self.snowflake_config, "Snowflake schema not found in the config file"

    def open_connection(self):
        """
        Open a connection to the Snowflake database.
        """
        # Connect to Snowflake
        ctx = snowflake.connector.connect(
            user=self.snowflake_config['user'],
            password=self.snowflake_config['password'],
            account=self.snowflake_config['account'],
            role=self.snowflake_config['role'],
        )

        # Connect to the database
        cs = ctx.cursor()
        cs.execute(f"USE WAREHOUSE {self.snowflake_config['warehouse']}")
        cs.execute(f"USE DATABASE {self.snowflake_config['database']}")
        cs.execute(f"USE SCHEMA {self.snowflake_config['schema']}")

        return ctx, cs

    def get_tables(self):
        """
        Get a list of table names in the Snowflake database.
        """
        # Get connection and cursor
        ctx, cs = self.open_connection()

        # fetch table names
        cs.execute("SHOW TABLES")
        tables = [table[1] for table in cs.fetchall()]

        return tables
    
    def get_columns_of_table(self, table_name):
        """
        Get a list of column names for a given table in the Snowflake database.
        """

        # Get connection and cursor
        ctx, cs = self.open_connection()

        # fetch table info
        cs.execute(f"DESCRIBE TABLE {table_name}")
        columns = [column[0] for column in cs.fetchall()]

        return columns
    
    def schema_dictionary(self, include_views=False):
        """
        Get schema of the Snowflake database in dictionary format. Table (and view) name is the key and the list of column names is the value.
        include_views: If True, include view names as keys in the schema dictionary.
        """
        schema = {}
    
        # Get connection and cursor
        ctx, cs = self.open_connection()

        # fetch table names
        cs.execute("SHOW TABLES")
        tables = [str(table[1].lower()) for table in cs.fetchall()]

        # fetch view names
        if include_views:
            cs.execute("SHOW VIEWS")
            views = [str(view[1].lower()) for view in cs.fetchall()]
            tables.extend(views)

        # fetch table info
        for table in tables:
            cs.execute(f"DESCRIBE TABLE {table}")
            schema[table] = [str(column[0].lower()) for column in cs.fetchall()]

        # Close the connection
        cs.close()
        ctx.close()

        return schema
    
    def schema_wording(self, selected_tables=None, include_sample_data=True, sample_size=5):
        """
        Generate a textual description of the schema of the Snowflake database, in the form of Data Definition Language (DDL) statements.
        """
        # Get connection and cursor
        ctx, cs = self.open_connection()

        # Query to get the tables
        tables = self.get_tables()

        # Readout the schema
        DDL = ''
        for table_id, table_name in enumerate(tables):
            if selected_tables and table_name not in selected_tables:
                continue
            DDL += f"CREATE TABLE {table_name} (\n"

            # Column details
            cs.execute(f"DESCRIBE TABLE {table_name}")
            columns = cs.fetchall()
                  
            # Column details
            for column_id, column in enumerate(columns):
                DDL += f"  {column[0]} {column[1]}"
                pk = column[5]
                if pk == 'Y':
                    DDL += " PRIMARY KEY"
                if column_id < len(columns) - 1:
                    DDL += ","
                DDL += "\n"

            # Foreign key details
            cs.execute(f"SHOW IMPORTED KEYS IN TABLE {table_name}")
            fks = cs.fetchall()
            for fk in fks:
                _, _, _, fk_table, fk_to, _, _, _, fk_from, _, _, _, _, _, _, _, _ = fk
                DDL += f"  FOREIGN KEY ({fk_from}) REFERENCES {fk_table}({fk_to})\n"

            DDL += ");\n\n"

            # Sample data
            if include_sample_data:
                cs.execute(f"SELECT * FROM {table_name}")
                # Handle the case where the table is empty
                if not cs.rowcount:
                    DDL += f"-- Sample Data: No sample data available\n\n"
                else:
                    data = cs.fetchmany(sample_size)
                    DDL += f"-- Sample Data:\n"
                    for row in data:
                        DDL += f"{row}\n"

            DDL += "\n"

        # Close the connection
        cs.close()
        ctx.close()

        return DDL

    def schema_wording_simple(self, selected_tables=None, include_sample_data=True, sample_size=5):
        """
        Generate a textual description of the schema of the Snowflake database.
        """
        # Get connection and cursor
        ctx, cs = self.open_connection()
        
        # Query to get the tables
        tables = self.get_tables()

        # Readout the schema
        schema = ''
        for table_id, table_name in enumerate(tables):
            if selected_tables and table_name not in selected_tables:
                continue
            schema += f"Table: {table_name}\n"
            schema += "=" * (7 + len(table_name)) + "\n"

            # Column details
            cs.execute(f"DESCRIBE TABLE {table_name}")
            columns = cs.fetchall()

            # Column details
            for column in columns:
                schema += f"Column: {column[0]}\n"
                schema += f"  Type: {column[1]}\n"
                pk = column[5]
                if pk == 'Y':
                    schema += f"  Primary Key\n"

            # Foreign key details
            cs.execute(f"SHOW IMPORTED KEYS IN TABLE {table_name}")
            fks = cs.fetchall()
            for fk in fks:
                _, _, _, fk_table, fk_to, _, _, _, fk_from, _, _, _, _, _, _, _, _ = fk
                schema += f"Foreign key {fk_from} references the primary key {fk_to} of table {fk_table}\n"

            # Sample data
            if include_sample_data:
                cs.execute(f"SELECT * FROM {table_name}")
                # Handle the case where the table is empty
                if not cs.rowcount:
                    schema += "Sample Data: No sample data available\n"
                else:
                    data = cs.fetchmany(min(sample_size, cs.rowcount))
                    schema += "Sample Data:\n"
                    for row in data:
                        schema += f"  {row}\n"
            
            schema += "\n"
        
        # Close the connection
        cs.close()
        ctx.close()

        return schema
    
    def schema_graph(self):
        """
        Generate a networkx graph object representing the schema of the Snowflake database.
        """
        # Get connection and cursor
        ctx, cs = self.open_connection()

        # Query to get the tables
        tables = self.get_tables()

        # Query to get the foreign-primary key pairs
        fk_pk_pairs = []
        for table_id, table_name in enumerate(tables):
            cs.execute(f"DESCRIBE TABLE {table_name}")
            columns = cs.fetchall()
            for column in columns:
                if column[5] == 'Y':
                    pk_table = table_name
                    pk_column = column[0]
                    break
            cs.execute(f"SHOW IMPORTED KEYS IN TABLE {table_name}")
            fks = cs.fetchall()
            for fk in fks:
                _, _, _, fk_table, fk_to, _, _, _, fk_from, _, _, _, _, _, _, _, _ = fk
                fk_table_id = tables.index(fk_table)
                fk_pk_pairs.append((table_id, table_name, fk_table_id, fk_table, fk_from, fk_to))
        
        # Close the connection
        cs.close()
        ctx.close()

        # Make the schema graph 
        G = nx.DiGraph()

        # Create nodes representing tables
        for table_id, table_name in enumerate(tables):
            G.add_node(table_id, name=table_name)

        # Create edges representing foreign-primary key pairs
        for table_id, _, fk_table_id, _, _, _ in fk_pk_pairs:
            G.add_edge(table_id, fk_table_id)
            G.add_edge(fk_table_id, table_id)
        
        return G
    
    def materialize_view(self, view_definition, verbose=True, persist=False):
        """
        Materialize a view in the Snowflake database.
        """
        
        # Get connection and cursor
        ctx, cs = self.open_connection()
        
        # Get the view name
        view_name = get_view_name_from_definition(view_definition)

        # Materialize the view
        if verbose:
            print("Materializing the view...")    
        done = False
        while not done:
            try:
                cs.execute(view_definition)
                done = True
            except Exception as e:
                # If the error says the view already exists, drop the view and try again
                if "already exists" in str(e):
                    if verbose:
                        print(f"View {view_name} already exists. Dropping the view and running the query again...")
                    cs.execute(f"DROP VIEW {view_name}")
                # Else return the error
                else:
                    if verbose:
                        print(f"Error in creating view {view_name}. Error received:\n{e}.")
                    return f"Error in creating view {view_name}. Error received:\n{e}."
                
        # Drop the view if not persisting
        if not persist:
            cs.execute(f"DROP VIEW {view_name}")
        
        if verbose:
            if persist:
                print(f"View {view_name} was defined successfully.")
            else:
                print(f"View {view_name} was defined successfully. The view has been dropped.")

        # Close the connection
        cs.close()
        ctx.close()

        return f"View {view_name} successfully defined."
import os
import re
import random
import sqlite3
import pandas as pd
import networkx as nx


def schema_graph(db_dir, save_dir=None):
    # Connect to the SQLite database
    conn = sqlite3.connect(db_dir)
    cursor = conn.cursor()

    # Query to get the tables
    cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
    tables = cursor.fetchall()
    table_names = [table[0] for table in tables]

    # Query to get the foreign-primary key pairs
    fk_pk_pairs = []
    for table_id, table_name in enumerate(table_names):
        cursor.execute(f"PRAGMA foreign_key_list({table_name});")
        fks = cursor.fetchall()
        for fk in fks:
            fk_id, fk_seq, fk_table, fk_from, fk_to, fk_on_update, fk_on_delete, fk_match = fk
            fk_table_id = [table_name for (table_name, ) in tables].index(fk_table)
            fk_pk_pairs.append((table_id, table_name, fk_table_id, fk_table, fk_from, fk_to))
    
    # Close the connection
    conn.close()
    
    # Make the schema graph 
    G = nx.DiGraph()

    # Create nodes representing tables
    for table_id, table_name in enumerate(table_names):
        G.add_node(table_id, name=table_name)

    # Create edges representing foreign-primary key pairs
    for table_id, _, fk_table_id, _, _, _ in fk_pk_pairs:
        G.add_edge(table_id, fk_table_id)
        G.add_edge(fk_table_id, table_id)

    if save_dir is not None:
        # Save the schema graph
        assert False, "Not implemented yet."
    
    return G


def sample_connected_subgraph_nodes(G, size):
    """
    Samples a random connected subgraph of size 'size' from a networkx graph.
    """

    # Start with a random node
    node = random.choice(list(G.nodes))
    subgraph_nodes = [node]

    # Grow the subgraph until it reaches the desired size
    while len(subgraph_nodes) < size:
        neighbors_1_hop = [neighbor[1] for neighbor in nx.bfs_edges(G, node, depth_limit=1)]
        neighbors = list(set(neighbors_1_hop) - set(subgraph_nodes))
        if neighbors:
            node = random.choice(neighbors)
            subgraph_nodes.append(node)
        # If no new neighbors, start from a random node in the subgraph
        elif len(set(subgraph_nodes) - {node}):
            node = random.choice(list(set(subgraph_nodes) - {node}))
            subgraph_nodes.append(node)
        # If no new neighbors and no other nodes in the subgraph, start from a random node in the graph
        elif len(set(G.nodes) - set(subgraph_nodes)):
            node = random.choice(list(set(G.nodes) - set(subgraph_nodes)))
            subgraph_nodes.append(node)
        # If no new neighbors and no other nodes in the graph, break
        else:
            break

    subgraph = G.subgraph(subgraph_nodes).copy()

    return subgraph


def schema_subgraph(G, n_nodes=5):
    """
    Returns a subgraph of the schema graph with n_nodes.
    """

    # Get the subgraph
    if n_nodes > len(G.nodes):
        raise ValueError(f"n_nodes ({n_nodes}) should be less than the number of nodes in the schema graph ({len(G.nodes)}).")
    subgraph = sample_connected_subgraph_nodes(G, n_nodes)

    # Get the subgraph nodes
    subgraph_nodes = G.subgraph(subgraph).nodes
    
    # Retrieve sampled table names
    sampled_tables = [G.nodes[node]['name'] for node in subgraph_nodes] 

    # Return the sampled tables
    return sampled_tables


if __name__ == '__main__':
    pass
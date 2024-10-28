import pandas as pd
import time
from code.db_utils import *
from code.queries import *
from itertools import combinations

# This method checks if left_attrs functionally determine right_attr.
# It counts if one unique value of right_attr, then left_attrs functionally determine right_attr.
def check_fd(df, left_attrs, right_attr):
    # If left_attrs is empty, return False
    if not left_attrs:  
        return False
    grouped = df.groupby(list(left_attrs))[right_attr].nunique()
    return (grouped == 1).all()

# This method builds a lattice of functional dependencies.
def build_lattice(df):
    column_names = df.columns.tolist()
    lattice = {}
    # Create nodes for combinations of attributes (up to 2 attributes on LHS) and initialize an empty list for dependencies
    for r in range(1, 3):
        for left_attrs in combinations(column_names, r):
            lattice[left_attrs] = []  
    # Check for functional dependencies and skip if right attr is part of left attrs
    for left_attrs in lattice.keys():
        for right_attr in column_names:
            if right_attr in left_attrs:
                continue 
            if check_fd(df, left_attrs, right_attr):
                lattice[left_attrs].append(right_attr)
    return lattice

# This method prunes redundant functional dependencies.
def prune_dependencies(df, lattice):
    pruned = {}
    for left_attrs, dependencies in lattice.items():
        pruned[left_attrs] = []
        for right_attr in dependencies:
            # Check if the dependency can be pruned, excluding trival dependencies
            if not any(check_fd(df, [attr for attr in left_attrs if attr != a], right_attr) for a in left_attrs):
                pruned[left_attrs].append(right_attr)
    return pruned

# print to screen
def print_dependencies(lattice):
    for left_attrs, dependencies in lattice.items():
        for right_attr in dependencies:
            print(f"{left_attrs} -> {right_attr}")

if __name__ == "__main__":
    total_start_time = time.time()

    print("========================================")
    print("Reading Data from Joined Table")
    #  get the data and list of columns
    df_joined_posts = exec_get_all(selection_q2)
    columns = exec_get_all(columns_q2)
    # convert the data into data frame to speed up computations
    df_joined_posts = pd.DataFrame(df_joined_posts, columns=[col[0] for col in columns])
    # remove the artificifically created primary id
    if "joinid" in df_joined_posts.columns:
        df_joined_posts.drop(columns=['joinid'], inplace=True)
    # get the final list of columns from the frame
    column_names = df_joined_posts.columns.tolist()
    print(f"Columns Loaded: {column_names}")
    print("Read Complete")
    print("========================================")

    # Build the lattice and test functional dependencies
    lattice = build_lattice(df_joined_posts)
    # Print discovered functional dependencies before pruning
    print("Discovered Functional Dependencies (before pruning):")
    print_dependencies(lattice)
    # Prune the functional dependencies
    pruned_lattice = prune_dependencies(df_joined_posts, lattice)
    # Print pruned functional dependencies
    print("\nDiscovered Functional Dependencies (after pruning):")
    print_dependencies(pruned_lattice)

    total_end_time = time.time()
    total_run_time = total_end_time - total_start_time
    print("Total running time: ", total_run_time, " seconds")

import pandas as pd
from itertools import combinations

class LatticeNode:
    def __init__(self, attrs):
        self.attrs = attrs
        self.dependencies = []  # Stores discovered dependencies

def check_fd(df, left_attrs, right_attr):
    """Check if left_attrs functionally determine right_attr."""
    grouped = df.groupby(list(left_attrs))[right_attr].nunique()
    return (grouped == 1).all()

def build_lattice(df):
    """Build a lattice of functional dependencies."""
    column_names = df.columns.tolist()
    lattice = {}
    
    # Create nodes for combinations of attributes
    for r in range(1, 3):  # Max 2 attributes on LHS
        for left_attrs in combinations(column_names, r):
            lattice[left_attrs] = LatticeNode(left_attrs)
    
    # Check for functional dependencies
    for left_attrs, node in lattice.items():
        for right_attr in column_names:
            if right_attr in left_attrs:
                continue  # Skip if right attr is part of left attrs
            
            if check_fd(df, left_attrs, right_attr):
                node.dependencies.append(right_attr)

    return lattice

def print_lattice(lattice):
    """Print the functional dependencies in the lattice."""
    for left_attrs, node in lattice.items():
        if node.dependencies:
            for right_attr in node.dependencies:
                print(f"{left_attrs} -> {right_attr}")

if __name__ == "__main__":
    # Example DataFrame (replace this with your actual data retrieval)
    data = {
        'A': [1, 1, 2, 2],
        'B': [1, 2, 1, 2],
        'C': [1, 1, 2, 2],
        'D': [1, 1, 1, 1]
    }
    df = pd.DataFrame(data)

    # Build the lattice
    lattice = build_lattice(df)

    # Print discovered functional dependencies in the lattice
    print("Discovered Functional Dependencies:")
    print_lattice(lattice)

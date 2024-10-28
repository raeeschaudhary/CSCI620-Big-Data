from code.db_utils import *
from code.queries import *
import time
from itertools import combinations
import pandas as pd

# This function checks if the specified left_attrs functionally determine the right_attr. 
# It counts if one unique value of right_attr, then left_attrs functionally determine right_attr.
def check_fd(df, left_attrs, right_attr):
    grouped = df.groupby(list(left_attrs))[right_attr].nunique()
    return (grouped == 1).all()

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

    # Generate combinations of attributes for testing functional dependencies
    combos = []
    for i in range(1, len(column_names)):
        multicol = combinations(column_names, i)
        combos.extend(multicol)

    functional_dependencies = []

    counter = 0
    # Check for functional dependencies
    for left_attrs in combos:
        counter += 1
        # comment/uncomment the following to run for fixed/total combinations
        if counter > 15:
            break
        for right_attr in column_names:
            print("exploring ", left_attrs, " with ", right_attr)
            if right_attr in left_attrs:
                continue  # Skip if right attr is part of left attrs
            # check the functional dependency using the frame; left hand and right hand side
            if check_fd(df_joined_posts, left_attrs, right_attr):
                functional_dependencies.append((left_attrs, right_attr))

    # Print discovered functional dependencies
    # found dependencies are printed on screnn and also provided in the txt file. q2_fds.txt
    print("\nDiscovered Functional Dependencies:")
    for left_attrs, right_attr in functional_dependencies:
        print(f"{left_attrs} -> {right_attr}")

    total_end_time = time.time()
    total_run_time = total_end_time - total_start_time
    print("Total running time: ", total_run_time, " seconds")

import pandas as pd
import time
from code.db_utils import *
from code.queries import *
from itertools import combinations

def build_lattice(col):
    '''
    Creates a lattice in form of a unidirectional graph
    :param col: Column names
    :return: lattice of attributes using itertools
    '''
    lattice = {}
    # Initialize the lattice with empty lists for each attribute
    for attribute in col:
        lattice[attribute] = []
        
        # Iterate over all combinations of column names taken two at a time
        for combo in combinations(col, 2):
            if attribute in combo:
                lattice[attribute].append(combo)

    return lattice


def refinement_better(x, z):
    '''
    Used the FD properties for partition refinement from the paper mentioned in writeup
    :param x: x's partition
    :param y: y's partions
    :param z: x U y partions
    :return: return true if x refine y
    '''
    a = x.indices
    b = z.indices
    if (len(a) == len(b)):
        return True
    else:
        return False

def pruning():
    '''
    Main function which controls the flow an also does the pruning
    Uncomment the lines if other refinement function need to e called
    :return: None
    '''
    import time

    t0 = time.time()

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

    
    lattice = build_lattice(column_names)
    print("got lattice")

    
    fd = {}
    allfds = {}
    for i in column_names:
        print("exploring i: ", i)
        visited = set()
        for k, v in lattice.items():
            if i == k or k in visited:
                visited.add(k)
                for others in v:  ## lattice pruning
                    visited.add(others)
                continue
            x = df_joined_posts.groupby(k, dropna=False)
            z = df_joined_posts.groupby([k, i], dropna=False)

            refined = refinement_better(x, z)
            if refined is True:
                if k not in fd.keys():
                    fd[k] = []
                    fd[k].append(i)
                else:
                    fd[k].append(i)
                visited.add(k)
                for others in v:  ## lattice pruning
                    visited.add(others)
        for k, v in lattice.items():
            for others in v:
                flag = 0
                for oth in others:
                    if oth == i:
                        flag = 1
                if others not in visited and flag == 0:
                    lhs = list(others)
                    x = df_joined_posts.groupby(lhs, dropna=False)
                    lhs.append(i)
                    z = df_joined_posts.groupby(lhs, dropna=False)

                    refined = refinement_better(x, z)
                    if refined is True:
                        if others not in fd.keys():
                            fd[others] = []
                            fd[others].append(i)
                        else:
                            fd[others].append(i)
                        visited.add(others)
        allfds[i] = visited

    print("Non inferable FD(Canonical) are printed .......")

    with open('noninferable_fd.txt', 'w') as f:
        for i, j in fd.items():
            for j2 in j:
                print(str(i) + "----->" + str(j2))
                f.write(str(i) + "----->" + str(j2))
                f.write('\n')

    print("Saved all the fds in allfd.txt .......")
    with open('allfd.txt', 'w') as f:
        for i, j in allfds.items():
            for j1 in j:
                f.write(str(j1) + "----->" + str(i))
                f.write('\n')

    t1 = time.time()

    print("IT took " + str(round((t1 - t0) / 60, 2)) + " mins")


if __name__ == '__main__':
    print("Finding FDs using lattice and pruning.......")
    pruning()
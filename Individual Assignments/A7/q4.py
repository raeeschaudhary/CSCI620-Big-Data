from sql.db_utils import exec_get_one, exec_commit, exec_get_all
from sql.queries import q1_level1, q4_lattice_query, q4_final_query
import time

if __name__=="__main__":    
    print('++++++++++++++++++++++++++++++++++++++++++++++')
    start_time = time.time()
    # Lattice N
    print('Creating Lattice L_N starting from L1')
    # level 1
    print('Creating L1, Itemset of size 1 with support of 100')
    exec_commit(q1_level1)
    print('Frequest Itemsets created as Table: L1')
    print('++++++++++++++++++++++++++++++++++++++++++++++')
    # get result count from L1
    res_count = exec_get_one("SELECT COUNT(*) FROM L1;")
    print(f"L1 created with {res_count[0]} itemsets.")
    # generating higher results for higher levels (L2, L3, ... LN)
    level = 2
    while True:
        # set lattice levels for reference
        prev_level = f"L{level - 1}"
        curr_level = f"L{level}"
        # create the query based on current and previous levels
        query = q4_lattice_query(level, prev_level, curr_level)
        # umcomment the following print if you want to see each generated query
        # print(query)
        # Execute the query
        exec_commit(query)
        # get result count from current level
        res_count = exec_get_one(f"SELECT COUNT(*) FROM {curr_level};")
        print(f"{curr_level} created with {res_count[0]} itemsets.")
        # stop if the current level is empty
        if res_count[0] == 0:
            print(f"Stopped at {curr_level} as it has no frequent itemsets.")
            break
        # else move to the next level
        level += 1

    # after break, we have the level set to last executed level (as we started from 2); take the final level - 1 (as last one should be empty)
    final_level = f"L{level - 1}"
    print(f"Last non-empty level: {final_level}, Final Query: ", end="")
    # create the final query
    f_query = q4_final_query(level, final_level)
    # print the final query for reporting 
    print(f_query)
    final_set = exec_get_all(f_query)
    # print frequent itemsets
    for item_set in final_set:
        print(item_set)

    print('++++++++++++++++++++++++++++++++++++++++++++++')
    print(f"Total running time: {time.time() - start_time} seconds")
    print('++++++++++++++++++++++++++++++++++++++++++++++')

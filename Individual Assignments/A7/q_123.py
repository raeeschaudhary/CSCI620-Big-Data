from sql.db_utils import exec_commit, report_db_statistics
from sql.queries import q1_level1, q2_level2, q3_level3
from globals import itemset_tables
import time

if __name__=="__main__":
    start_time = time.time()
    
    print('++++++++++++++++++++++++++++++++++++++++++++++')
    # level 1
    print('Creating L1, Itemset of size 1 with support of 100')
    exec_commit(q1_level1)
    print('Frequest Itemsets created as Table: L1')
    print('++++++++++++++++++++++++++++++++++++++++++++++')
    # level 2
    print('Creating L2, Itemset of size 2 with support of 100, based on L1')
    exec_commit(q2_level2)
    print('Frequest Itemsets created as Table: L2')
    print('++++++++++++++++++++++++++++++++++++++++++++++')
    # level 13
    print('Creating L3, Itemset of size 3 with support of 100, based on L2')
    exec_commit(q3_level3)
    print('Frequest Itemsets created as Table: L3')
    print('++++++++++++++++++++++++++++++++++++++++++++++')
    # report count
    print('Itemsets and Data Count')
    report_db_statistics(itemset_tables)
    print('++++++++++++++++++++++++++++++++++++++++++++++')

    end_time = time.time()
    run_time = end_time - start_time
    print("Total running time: ", run_time, " seconds")

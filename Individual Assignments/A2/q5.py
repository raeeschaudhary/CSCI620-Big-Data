from code.db_utils import *
from code.queries import *
import time


if __name__=="__main__":
    total_start_time = time.time()


    print('++++++++++++++++++++++++++++++++++++++++++++++')
    print("Creating Indexes for Query Optimization")
    exec_commit(indexing_queries)
    print("Index Created. Restart the database and run the q3.py again to observe the effect")
    print('++++++++++++++++++++++++++++++++++++++++++++++')
    
    total_end_time = time.time()
    total_run_time = total_end_time - total_start_time
    print("Total running time: ", total_run_time, " seconds")

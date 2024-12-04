from mongo.db_methods import kmeans_step_execution
from globals import K, T
import time

if __name__=="__main__":
    start_time = time.time()
    
    print('++++++++++++++++++++++++++++++++++++++++++++++')

    # running a k-means step with a tag to update clusters, can be tested to run for more than one stange by increasing the loop range
    for i in range(1):
        print(f"Executing k-means step {i + 1} to update centroids and clusters")
        print('K = sample size, T = tag; set in globals.py')
        # kmeans_step_execution(K, T)
        kmeans_step_execution(K, T)
        print('centroids re-cacluated and clusters updated successfully')
        print('++++++++++++++++++++++++++++++++++++++++++++++')
        # uncomment following to run step wise and see changes in db
        # input("Press Enter to continue...")

    end_time = time.time()
    run_time = end_time - start_time
    print("Total running time: ", run_time, " seconds")

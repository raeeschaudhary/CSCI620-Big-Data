from mongo.db_methods import create_centroids, report_db_statistics
from globals import K, T
import time

if __name__=="__main__":
    start_time = time.time()
    
    print('++++++++++++++++++++++++++++++++++++++++++++++')
    # create subset collection called centroids
    print('Creating collections centroids with kNorms: ')
    print('K = sample size, T = tag; set in globals.py')
    print('++++++++++++++++++++++++++++++++++++++++++++++')
    create_centroids(K, T)
    print(f'centroids collection created for {T} with {K} clusters.')
    print('++++++++++++++++++++++++++++++++++++++++++++++')
    # report db statistics 
    print('++++++++++++++++++++++++++++++++++++++++++++++')
    print("Report DB Statistics")
    report_db_statistics()
    print('++++++++++++++++++++++++++++++++++++++++++++++')

    end_time = time.time()
    run_time = end_time - start_time
    print("Total running time: ", run_time, " seconds")

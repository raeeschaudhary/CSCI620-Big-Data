from mongo.db_methods import run_kmeans_iterations
import time
from globals import subset_tags

if __name__=="__main__":
    start_time = time.time()
    
    print('++++++++++++++++++++++++++++++++++++++++++++++')
    # running a k-means step with a tag to update clusters
    print('Running K-Means for upto 100 iterations (or if convergves before) ')
    print('Subset tags for each tag; set in globals.py')
    print('K = 10 to 50 with step of 5.')
    run_kmeans_iterations(subset_tags)
    print('All graphs saved in root folder successfully, view to get an idea of best K')
    print('++++++++++++++++++++++++++++++++++++++++++++++')
    
    end_time = time.time()
    run_time = end_time - start_time
    print("Total running time: ", run_time, " seconds")

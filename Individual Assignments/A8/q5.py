from mongo.db_methods import run_kmeans_best_cluster
import time
from globals import subset_tags, best_Ks

if __name__=="__main__":
    start_time = time.time()
    
    print('++++++++++++++++++++++++++++++++++++++++++++++')
    # running a k-means for all tags for best values of K
    print('Running best cluster for each tag')
    for i in range(len(subset_tags)):    
        print(f'Running for Tag {subset_tags[i]} with K = {best_Ks[i]}')
        run_kmeans_best_cluster(subset_tags[i], best_Ks[i])
    print('Records saved in collections. Analysis is explained in the report.')
    print('++++++++++++++++++++++++++++++++++++++++++++++')

    end_time = time.time()
    run_time = end_time - start_time
    print("Total running time: ", run_time, " seconds")

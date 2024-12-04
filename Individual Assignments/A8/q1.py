from mongo.db_methods import create_subset_posts, normalize_kmposts, report_db_statistics
import time
from globals import subset_tags


if __name__=="__main__":
    start_time = time.time()
    
    print('++++++++++++++++++++++++++++++++++++++++++++++')
    # create subset collection called kmposts
    print('Creating subset of posts collection; kmposts with following tags: ')
    print('["command-line", "boot", "networking", "apt", "drivers"]')
    create_subset_posts('kmposts', subset_tags)
    print('kmposts collection created')
    print('++++++++++++++++++++++++++++++++++++++++++++++')
    # calcualte kmeansNorm and update kmposts
    print('Calculating and inserting normalized values for view count and score')
    normalize_kmposts('kmposts')
    print('kmposts updated with normalized kmeans field')
    print('++++++++++++++++++++++++++++++++++++++++++++++')
    # report db statistics 
    print('++++++++++++++++++++++++++++++++++++++++++++++')
    print("Report DB Statistics")
    report_db_statistics()
    print('++++++++++++++++++++++++++++++++++++++++++++++')

    end_time = time.time()
    run_time = end_time - start_time
    print("Total running time: ", run_time, " seconds")

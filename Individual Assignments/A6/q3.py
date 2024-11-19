from mongo.db_methods import perform_matching_and_report
import time

if __name__=="__main__":
    start_time = time.time()
    
    print('++++++++++++++++++++++++++++++++++++++++++++++')

    print('Matching extra data with different attributes')
    perform_matching_and_report("extra-data.json")
    print('Matching process completed')
    print('++++++++++++++++++++++++++++++++++++++++++++++')


    end_time = time.time()
    run_time = end_time - start_time
    print("Total running time: ", run_time, " seconds")

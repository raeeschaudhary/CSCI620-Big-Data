from code.db_utils import *
from code.queries import *
import time

if __name__=="__main__":
    total_start_time = time.time()
    
    q_start_time = time.time()
    print('++++++++++++++++++++++++++++++++++++++++++++++')
    print("Running Queries and Results")
    print("Names of the top 10 most popular badges earned by users within a year of creating their accounts.")
    results = exec_get_all(q2_query1)
    if results:
        count = 0
        for row in results:
            print(row)
            count = count + 1
            if count > 10:
                break
    print('++++++++++++++++++++++++++++++++++++++++++++++')
    print("Query took: ", time.time() - q_start_time, " seconds")


    q_start_time = time.time()
    print('++++++++++++++++++++++++++++++++++++++++++++++')
    print("Running Queries and Results")
    print("Display names of users who have never posted but have a reputation greater than 1,000.")
    results = exec_get_all(q2_query2)
    if results:
        count = 0
        for row in results:
            print(row)
            count = count + 1
            if count > 10:
                break
    print('++++++++++++++++++++++++++++++++++++++++++++++')
    print("Query took: ", time.time() - q_start_time, " seconds")


    q_start_time = time.time()
    print('++++++++++++++++++++++++++++++++++++++++++++++')
    print("Running Queries and Results")
    print("Display name and reputation of users who have answered more than one question with the tag postgresql.")
    results = exec_get_all(q2_query3)
    if results:
        count = 0
        for row in results:
            print(row)
            count = count + 1
            if count > 10:
                break
    print('++++++++++++++++++++++++++++++++++++++++++++++')
    print("Query took: ", time.time() - q_start_time, " seconds")

    q_start_time = time.time()
    print('++++++++++++++++++++++++++++++++++++++++++++++')
    print("Running Queries and Results")
    print("Display name of users who posted comments with a score greater than 10 within the first week of creating their accounts.")
    results = exec_get_all(q2_query4)
    if results:
        count = 0
        for row in results:
            print(row)
            count = count + 1
            if count > 10:
                break
    print('++++++++++++++++++++++++++++++++++++++++++++++')
    print("Query took: ", time.time() - q_start_time, " seconds")

    q_start_time = time.time()
    print('++++++++++++++++++++++++++++++++++++++++++++++')
    print("Running Queries and Results")
    print("The tag names of the tags most commonly used on posts along with the tag postgresql and the count of each tag.")
    results = exec_get_all(q2_query5)
    if results:
        count = 0
        for row in results:
            print(row)
            count = count + 1
            if count > 10:
                break
    print('++++++++++++++++++++++++++++++++++++++++++++++')
    print("Query took: ", time.time() - q_start_time, " seconds")
    
    total_end_time = time.time()
    total_run_time = total_end_time - total_start_time
    print("Total running time: ", total_run_time, " seconds")

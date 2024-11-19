from sql.db_utils import exec_get_all
from sql.queries import q5_query1_materialized, q5_query1_non_materialized, q5_query2_materialized, q5_query2_non_materialized
import time

if __name__=="__main__":    
    print('++++++++++++++++++++++++++++++++++++++++++++++')

    # Query 1
    print('Query 1: Users with reputation more than 100 who have commented on at least 10 posts.')
    print('++++++++++++++++++++++++++++++++++++++++++++++')
    print("Running Materialized Version")
    q_start_time = time.time()
    result = exec_get_all(q5_query1_materialized, None)
    counter = 1
    print('++++++++++++++++++++++++++++++++++++++++++++++')
    for record in result:
        print(record)
        counter += 1
        if counter > 10:
            break
    print('++++++++++++++++++++++++++++++++++++++++++++++')
    print(f"Total running time: {time.time() - q_start_time} seconds")
    print('++++++++++++++++++++++++++++++++++++++++++++++')

    print('++++++++++++++++++++++++++++++++++++++++++++++')
    print("Running Non-Materialized Version")
    q_start_time = time.time()
    result = exec_get_all(q5_query1_non_materialized, None)
    counter = 1
    print('++++++++++++++++++++++++++++++++++++++++++++++')
    for record in result:
        print(record)
        counter += 1
        if counter > 10:
            break
    print('++++++++++++++++++++++++++++++++++++++++++++++')
    print(f"Total running time: {time.time() - q_start_time} seconds")
    print('++++++++++++++++++++++++++++++++++++++++++++++')


    # Query 2
    print('Query 2: Users whose display name starts with "john-" and who have never commented on any post with the tag "networking".')
    print('++++++++++++++++++++++++++++++++++++++++++++++')
    print("Running Materialized Version")
    q_start_time = time.time()
    result = exec_get_all(q5_query2_materialized, None)
    counter = 1
    print('++++++++++++++++++++++++++++++++++++++++++++++')
    for record in result:
        print(record)
        counter += 1
        if counter > 10:
            break
    print('++++++++++++++++++++++++++++++++++++++++++++++')
    print(f"Total running time: {time.time() - q_start_time} seconds")
    print('++++++++++++++++++++++++++++++++++++++++++++++')

    print('++++++++++++++++++++++++++++++++++++++++++++++')
    print("Running Non-Materialized Version")
    q_start_time = time.time()
    result = exec_get_all(q5_query2_non_materialized, None)
    counter = 1
    print('++++++++++++++++++++++++++++++++++++++++++++++')
    for record in result:
        print(record)
        counter += 1
        if counter > 10:
            break
    print('++++++++++++++++++++++++++++++++++++++++++++++')
    print(f"Total running time: {time.time() - q_start_time} seconds")
    print('++++++++++++++++++++++++++++++++++++++++++++++')

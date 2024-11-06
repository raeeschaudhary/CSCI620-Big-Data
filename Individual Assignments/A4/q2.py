from funcs.queries import *
from funcs.db_methods import list_indexes_all
import time

if __name__=="__main__":
    print('++++++++++++++++++++++++++++++++++++++++++++++')
    print("Current Index List")
    indexes = list_indexes_all()
    for index in indexes:
        print(index)
    indexes = []

    start_time = time.time()
    # Running Query 1
    print('++++++++++++++++++++++++++++++++++++++++++++++')
    print('Names of the top 10 most popular badges earned by users within a year of creating their accounts.')
    q_time = time.time()
    badges = q2_query1()
    count = 0
    for badge in badges:
        print(f"Badge: {badge['badgeName']}, Count: {badge['count']}")
        count += 1
    print('++++++++++++++++++++++++++++++++++++++++++++++')
    print(f"Total records returned: {count}")
    print('++++++++++++++++++++++++++++++++++++++++++++++')
    print(f"Total running time: {time.time() - q_time} seconds")
    print('++++++++++++++++++++++++++++++++++++++++++++++')


    # Running Query 2
    print('++++++++++++++++++++++++++++++++++++++++++++++')
    print('Display names of users who have never posted but have a reputation greater than 1,000.')
    q_time = time.time()
    display_names = q2_query2()
    count = 0
    for name in display_names:
        print(f"User Display Name: {name['DisplayName']}")
        count += 1
    print('++++++++++++++++++++++++++++++++++++++++++++++')
    print(f"Total records returned: {count}")
    print('++++++++++++++++++++++++++++++++++++++++++++++')
    print(f"Total running time: {time.time() - q_time} seconds")
    print('++++++++++++++++++++++++++++++++++++++++++++++')

    # Running Query 3
    print('++++++++++++++++++++++++++++++++++++++++++++++')
    print('Display name and reputation of users who have answered more than one question with the tag postgresql.')
    q_time = time.time()
    names_reputation = q2_query3()
    count = 0
    for nam_rep in names_reputation:
        print(f"User Display Name: {nam_rep['DisplayName']}")
        count += 1
    print('++++++++++++++++++++++++++++++++++++++++++++++')
    print(f"Total records returned: {count}")
    print('++++++++++++++++++++++++++++++++++++++++++++++')
    print(f"Total running time: {time.time() - q_time} seconds")
    print('++++++++++++++++++++++++++++++++++++++++++++++')

    # Running Query 4
    print('++++++++++++++++++++++++++++++++++++++++++++++')
    print('Display name of users who posted comments with a score greater than 10 within the first week of creating their accounts.')
    q_time = time.time()
    names_10_score = q2_query4()
    count = 0
    for name in names_10_score:
        print(f"User Display Name: {name['DisplayName']}")
        count += 1
    print('++++++++++++++++++++++++++++++++++++++++++++++')
    print(f"Total records returned: {count}")
    print('++++++++++++++++++++++++++++++++++++++++++++++')
    print(f"Total running time: {time.time() - q_time} seconds")
    print('++++++++++++++++++++++++++++++++++++++++++++++')

    # Running Query 5
    print('++++++++++++++++++++++++++++++++++++++++++++++')
    print('The tag names of the tags most commonly used on posts along with the tag postgresql and the count of each tag.')
    q_time = time.time()
    tags_counts = q2_query5()
    count = 0
    for tag in tags_counts:
        print(f"Tag Name: {tag['_id']}, Count: {tag['count']}")
        count += 1
    print('++++++++++++++++++++++++++++++++++++++++++++++')
    print(f"Total records returned: {count}")
    print('++++++++++++++++++++++++++++++++++++++++++++++')
    print(f"Total running time: {time.time() - q_time} seconds")
    print('++++++++++++++++++++++++++++++++++++++++++++++')

    end_time = time.time()
    run_time = end_time - start_time
    print("Total running time: ", run_time, " seconds")

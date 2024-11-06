from funcs.queries import *
import pprint
import time

if __name__=="__main__":
    start_time = time.time()
    # Explaining Query 1
    print('++++++++++++++++++++++++++++++++++++++++++++++')
    print('Names of the top 10 most popular badges earned by users within a year of creating their accounts.')
    badges_explain = q2_query1(explain=True)
    pprint.pprint(badges_explain)
    with open("q3_1_plan.txt", "w") as file:
        file.write(pprint.pformat(badges_explain))
    print('++++++++++++++++++++++++++++++++++++++++++++++')

    # Explaining Query 2
    print('++++++++++++++++++++++++++++++++++++++++++++++')
    print('Display names of users who have never posted but have a reputation greater than 1,000.')
    display_names = q2_query2(explain=True)
    pprint.pprint(display_names)
    with open("q3_2_plan.txt", "w") as file:
        file.write(pprint.pformat(display_names))
    print('++++++++++++++++++++++++++++++++++++++++++++++')

    # Explaining Query 3
    print('++++++++++++++++++++++++++++++++++++++++++++++')
    print('Display name and reputation of users who have answered more than one question with the tag postgresql.')
    names_reputation = q2_query3(explain=True)
    pprint.pprint(names_reputation)
    with open("q3_3_plan.txt", "w") as file:
        file.write(pprint.pformat(names_reputation))
    print('++++++++++++++++++++++++++++++++++++++++++++++')

    # Explaining Query 4
    print('++++++++++++++++++++++++++++++++++++++++++++++')
    print('Display name of users who posted comments with a score greater than 10 within the first week of creating their accounts.')
    names_10_score = q2_query4(explain=True)
    pprint.pprint(names_10_score)
    with open("q3_4_plan.txt", "w") as file:
        file.write(pprint.pformat(names_10_score))
    print('++++++++++++++++++++++++++++++++++++++++++++++')

    # Running Query 5
    print('++++++++++++++++++++++++++++++++++++++++++++++')
    print('The tag names of the tags most commonly used on posts along with the tag postgresql and the count of each tag.')
    tags_counts = q2_query5(explain=True)
    pprint.pprint(tags_counts)
    with open("q3_5_plan.txt", "w") as file:
        file.write(pprint.pformat(tags_counts))
    print('++++++++++++++++++++++++++++++++++++++++++++++')

    end_time = time.time()
    run_time = end_time - start_time
    print("Total running time: ", run_time, " seconds")

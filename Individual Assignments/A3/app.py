from code.db_methods import *
from code.queries import *
import time

if __name__=="__main__":
    start_time = time.time()
    
    print('++++++++++++++++++++++++++++++++++++++++++++++')
    print("Checking if the DB is populated with base table")
    db = check_db_exists()
    if db == True:
        print('=================================================')
        print("DB already exists; No need to repopulate the data")
        print('=================================================')
    else:
        print('=================================================')
        print("Inserting data to populate the DB")
        print('=================================================')

        print('++++++++++++++++++++++++++++++++++++++++++++++')
        print('Creaing Schema')
        run_schema_script('create_schema.sql')
        print('Schema Created')
        print('++++++++++++++++++++++++++++++++++++++++++++++')

        print("Inserting Users")
        insert_users("Users.xml", users_query)
        print('Users Inserted')
        print('++++++++++++++++++++++++++++++++++++++++++++++')

        print("Inserting Tags")
        insert_tags("Tags.xml", tags_query)
        print('Tags Inserted')
        print('++++++++++++++++++++++++++++++++++++++++++++++')

        print("Inserting Posts and Post Tags")
        insert_posts("Posts.xml", posts_query, post_tags_query)
        print("Creating FK-Self Relation on Post")
        run_commit_query(final_query)
        print('Posts and Post Tags Inserted')
        print('++++++++++++++++++++++++++++++++++++++++++++++')

    print("DB Statistics")
    report_db_statistics()
    print('++++++++++++++++++++++++++++++++++++++++++++++')

    print("Creating the Join with Users, Posts, Tags, Post Tags")
    run_commit_query(join_tables_q1)
    print('++++++++++++++++++++++++++++++++++++++++++++++')

    

    end_time = time.time()
    run_time = end_time - start_time
    print("Total running time: ", run_time, " seconds")

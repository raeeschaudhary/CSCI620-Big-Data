from code.db_methods import *
from code.queries import *
import time

if __name__=="__main__":
    start_time = time.time()
    
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

    print("Inserting Badges")
    insert_badges("Badges.xml", badges_query)
    print('Badges Inserted')
    print('++++++++++++++++++++++++++++++++++++++++++++++')

    print("Inserting Posts and Post Tags")
    insert_posts("Posts.xml", posts_query, post_tags_query)
    print("Creating FK-Self Relation on Post")
    run_commit_query(final_query)
    print('Posts and Post Tags Inserted')
    print('++++++++++++++++++++++++++++++++++++++++++++++')

    print("Inserting Comments")
    insert_comments("Comments.xml", comments_query)
    print('Comments Inserted')
    print('++++++++++++++++++++++++++++++++++++++++++++++')

    print("Statistics")
    report_db_statistics()
    print('++++++++++++++++++++++++++++++++++++++++++++++')
    

    end_time = time.time()
    run_time = end_time - start_time
    print("Total running time: ", run_time, " seconds")

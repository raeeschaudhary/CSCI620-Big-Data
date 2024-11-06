from funcs.db_methods import *
from funcs.queries import *
import time

if __name__=="__main__":
    start_time = time.time()
    
    print('++++++++++++++++++++++++++++++++++++++++++++++')

    print('Deleting all data and collections in the database.')
    cleaning_database()
    print("Database cleaned. Ready for fresh insertion.")
    print('++++++++++++++++++++++++++++++++++++++++++++++')

    print('Recreating Collections (users, posts).')
    creating_collections()
    print("New Collections Created.")
    print('++++++++++++++++++++++++++++++++++++++++++++++')
    
    print("insert users")
    insert_users("Users.xml")
    print("\nusers inserted")
    print('++++++++++++++++++++++++++++++++++++++++++++++')

    print("adding badges in users")
    insert_badges("Badges.xml")
    print("\nbadges inserted inside users")
    print('++++++++++++++++++++++++++++++++++++++++++++++')

    print("insert posts")
    insert_posts("Posts.xml")
    print("\nposts inserted")
    print('++++++++++++++++++++++++++++++++++++++++++++++')
    
    print("adding comments in posts")
    insert_comments("Comments.xml")
    print("\ncomments inserted inside posts")
    print('++++++++++++++++++++++++++++++++++++++++++++++')

    print("Removing keys used for mapping")
    report_mapping_keys_users()
    report_mapping_keys_posts()
    print("keys removed successfully")
    print('++++++++++++++++++++++++++++++++++++++++++++++')

    print('++++++++++++++++++++++++++++++++++++++++++++++')
    print("Report DB Statistics")
    report_db_statistics()
    print('++++++++++++++++++++++++++++++++++++++++++++++')

    end_time = time.time()
    run_time = end_time - start_time
    print("Total running time: ", run_time, " seconds")

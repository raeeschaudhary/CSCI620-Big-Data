from mongo.db_methods import insert_accountid_users, insert_sites_data_users, report_db_statistics
import time

if __name__=="__main__":
    start_time = time.time()
    
    print('++++++++++++++++++++++++++++++++++++++++++++++')

    print('Inserting AccountID to users')
    insert_accountid_users("Users.xml")
    print('Account IDs added to users')
    print('++++++++++++++++++++++++++++++++++++++++++++++')

    print('Inserting Sites data to users')
    insert_sites_data_users("extra-data.json")
    print('Sites Data Added added to users')
    print('++++++++++++++++++++++++++++++++++++++++++++++')

    print('++++++++++++++++++++++++++++++++++++++++++++++')
    print("Report DB Statistics")
    report_db_statistics()
    print('++++++++++++++++++++++++++++++++++++++++++++++')

    end_time = time.time()
    run_time = end_time - start_time
    print("Total running time: ", run_time, " seconds")

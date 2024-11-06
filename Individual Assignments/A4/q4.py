from funcs.queries import q4_index_creation
from funcs.db_methods import list_indexes_all
import time

if __name__=="__main__":
    start_time = time.time()
    # creating indexes 
    print('++++++++++++++++++++++++++++++++++++++++++++++')
    print("Existing Indexes")
    indexes = list_indexes_all()
    for index in indexes:
        print(index)
    indexes = []
    print('Creating Index.')
    q4_index_creation()
    print("Indexes created succussfully. ")
    print('++++++++++++++++++++++++++++++++++++++++++++++')
    print("Indexes including the newly created.")
    indexes_all = list_indexes_all()
    for index in indexes_all:
        print(index)
    print('==============================================')
    print('Re-run q2.py and q3.py and observe the difference')
    print('==============================================')
    print('++++++++++++++++++++++++++++++++++++++++++++++')
    
    end_time = time.time()
    run_time = end_time - start_time
    print("Total running time: ", run_time, " seconds")

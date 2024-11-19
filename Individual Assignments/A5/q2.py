from sql.db_utils import exec_sql_file, report_db_statistics
from globals import m_gavs, non_m_gavs
import time

if __name__=="__main__":
    start_time = time.time()
    
    print('++++++++++++++++++++++++++++++++++++++++++++++')

    print('Creaing Global Schema with Materialized and Non-Materialized Views')
    exec_sql_file('create_schema_gav.sql')
    print('Global Schema Created')
    print('++++++++++++++++++++++++++++++++++++++++++++++')

    print('Non-Materialized Views and Data Count')
    report_db_statistics(non_m_gavs)
    print('++++++++++++++++++++++++++++++++++++++++++++++')
    
    print('Materialized Views and Data Count')
    report_db_statistics(m_gavs)
    print('++++++++++++++++++++++++++++++++++++++++++++++')

    end_time = time.time()
    run_time = end_time - start_time
    print("Total running time: ", run_time, " seconds")

from sql.db_utils import exec_sql_file, report_db_statistics
from globals import non_m_source_views, m_source_views
import time

if __name__=="__main__":
    start_time = time.time()
    
    print('++++++++++++++++++++++++++++++++++++++++++++++')

    print('Creaing Source Schema with Materialized and Non-Materialized Views')
    exec_sql_file('create_schema_basic.sql')
    print('Source Schema Created')
    print('++++++++++++++++++++++++++++++++++++++++++++++')

    print('Non-Materialized Views and Data Count')
    report_db_statistics(non_m_source_views)
    print('++++++++++++++++++++++++++++++++++++++++++++++')
    
    print('Materialized Views and Data Count')
    report_db_statistics(m_source_views)
    print('++++++++++++++++++++++++++++++++++++++++++++++')

    end_time = time.time()
    run_time = end_time - start_time
    print("Total running time: ", run_time, " seconds")

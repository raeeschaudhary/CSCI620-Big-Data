from code.db_utils import *
from code.queries import *
import time

def save_results_to_file(query_name, results):
    """Saves the results of a query to a text file."""
    with open(f"{query_name}_results.txt", "w") as file:
        for row in results:
            file.write(str(row) + "\n")

if __name__=="__main__":
    total_start_time = time.time()
    
    query_list = [
        (q3_query1_explain, "query1"),
        (q3_query2_explain, "query2"),
        (q3_query3_explain, "query3"),
        (q3_query4_explain, "query4"),
        (q3_query5_explain, "query5"),
    ]

    for query, query_name in query_list:
        print('++++++++++++++++++++++++++++++++++++++++++++++')
        print(f"Visualize {query_name.capitalize()}")
        results = exec_get_all(query)
        if results:
            save_results_to_file(query_name, results)  # Save results to a file
            print(f"Results saved to {query_name}_results.txt")
        else:
            print("No results found.")
        print('++++++++++++++++++++++++++++++++++++++++++++++')
    
    total_end_time = time.time()
    total_run_time = total_end_time - total_start_time
    print("Total running time: ", total_run_time, " seconds")

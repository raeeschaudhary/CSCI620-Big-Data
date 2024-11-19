import time
import json
from globals import data_directory

def update_dictionary(key, value, key_info):
    """
    Create a dictionary for each key value pair to build statistics.
    
    :param key: Key name (e.g. account_id).
    :param value: Value to add.
    :param key_info: Dictionary to store analysis results.
    """
    # check if the value is present
    value_str = str(value) if value is not None else ''
    # check the length of value - to extract data type or size
    value_length = len(value_str)
    # add values to dictionary using keys - e.g., long value, null counts, lengths, uniques
    if key not in key_info:
        key_info[key] = {
            # take any values as longest
            "longest_value": value_str,
            # take its length as longest
            "longest_value_length": value_length,
            # count null if there is no value
            "null_count": 1 if value is None else 0,
            # unique if the value is not none
            "unique_values": set([value_str]) if value is not None else set(),
            # take the first as unique 
            "unique_count": 1 if value is not None else 0
        }
    # if it is is not the first value; update dictionary with values.
    else:
        # check if value length is greater than any key value
        if value_length > key_info[key]["longest_value_length"]:
            # assign a new longest value and its length in dict
            key_info[key]["longest_value"] = value_str
            key_info[key]["longest_value_length"] = value_length
        # increment the null count if a value is none
        if value is None:
            key_info[key]["null_count"] += 1
        # increment the unique counts if the value is not present in unique values
        if value_str not in key_info[key]["unique_values"]:
            key_info[key]["unique_values"].add(value_str)
            key_info[key]["unique_count"] = len(key_info[key]["unique_values"])

def analyze_json(file_path):
    """
    Analyze JSON file to explore data descriptive statistics.
    
    :param file_path: Path to input JSON file.
    :returns: key_info (count, length, nulls, uniques) and count of total records.
    """
    # set the key_info dictionary 
    key_info = {}
    # counter for total records
    total_records = 0
    with open(file_path, 'r', encoding='utf-8') as file:
        for line in file:
            # read each line in the file
            data = json.loads(line)
            # increment the records
            total_records += 1  
            # Process each key-value
            for key, value in data.items():
                # badges is nested element inside badge_count
                if key == "badge_counts" and isinstance(value, dict):
                    # Add badge_counts to dictionary (bronze, silver, gold) for each entry
                    for sub_key, sub_value in value.items():
                        full_key = f"{key}.{sub_key}"
                        # update dict
                        update_dictionary(full_key, sub_value, key_info)
                else:
                    # Process other top-level keys
                    update_dictionary(key, value, key_info)
    # Remove the unique_values set before returning to keep output concise
    for key in key_info:
        del key_info[key]["unique_values"]
    # return key info and total records
    return key_info, total_records

if __name__=="__main__":
    start_time = time.time()
    
    print('++++++++++++++++++++++++++++++++++++++++++++++')
    # The same file name as downloaded is assumed
    input_file = 'extra-data.json'
    # save output file with analysis
    with open('file_info.txt', 'w') as f:
        # Place in data directory provided in globals.py
        file_path = data_directory + input_file
        print("Exploring: ", file_path)
        f.write("++++++++++++++++++++++++++++++++++++++++++++++++++++++++\n")
        f.write(f"File: {file_path}\n")
        # analyze the json file
        key_info, total_records = analyze_json(file_path)
        # write results
        f.write(f"Total Records: {total_records}\n")
        f.write("Key information with max value lengths, longest values, and null counts:\n")
        # write key info results
        for key, info in sorted(key_info.items()):
            f.write(f"Key: {key}\n")
            f.write(f"Longest Value Length: {info['longest_value_length']}\n")
            if info['longest_value_length'] > 1000:
                f.write(f"Longest Value: {info['longest_value']} (Greater than 1000 characters)\n")
            else:
                f.write(f"Longest Value: {info['longest_value']}\n")
            f.write(f"Null count: {info['null_count']}\n")
            f.write(f"Unique Count: {info['unique_count']}\n")
        f.write("++++++++++++++++++++++++++++++++++++++++++++++++++++++++\n")
        f.write("\n")
    # success message on screen. 
    print(f"Results stored in file_info.txt")

    end_time = time.time()
    run_time = end_time - start_time
    print("Total running time: ", run_time, " seconds")
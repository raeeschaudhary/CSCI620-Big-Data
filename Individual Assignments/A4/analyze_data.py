from funcs.globals import *
import xml.etree.ElementTree as ET

# This method analyze each XML file to check the values, data types, max_length, null count, and unique count for each attribut. 
def analyze_xml(file_path):
    key_info = {}
    total_records = 0
    # iterate over each row to get each key, value
    try:
        context = ET.iterparse(file_path, events=("end",))
        temp_unique_counts = {}
        for event, elem in context:
            if elem.tag == 'row':
                total_records += 1
                
                for key, value in elem.attrib.items():
                    value_length = len(value)
                    if key not in key_info:
                        key_info[key] = {
                            "max_length": value_length,
                            "longest_value": value,
                            "null_count": 0, 
                            "unique_count": 0 
                        }
                        temp_unique_counts[key] = set() 
                    else:
                        if value_length > key_info[key]["max_length"]:
                            key_info[key]["max_length"] = value_length
                            key_info[key]["longest_value"] = value
                        # take unique values; if unique values are more than 50 we skip that assuming it could be a big numeric or text value (not an unique type)
                    if key_info[key]["unique_count"] < 50:
                        if key not in temp_unique_counts:
                            temp_unique_counts[key] = set()
                        temp_unique_counts[key].add(value)
                        key_info[key]["unique_count"] = min(len(temp_unique_counts[key]), 50)
                # get the null count if key or value is not present 
                for key in key_info:
                    if key not in elem.attrib or elem.attrib[key] == "":
                        key_info[key]["null_count"] += 1

                elem.clear()  

    except Exception as e:
        print(f"Error processing file {file_path}: {e}")

    return key_info, total_records

# This method saves the exploration out to a text file for further manual analysis
def save_results_to_file(file_paths, output_file):
    with open(output_file, 'w') as f:
        for file in file_paths:
            file_path = data_directory + file + ".xml"
            print("Exploring: ", file_path)
            f.write("++++++++++++++++++++++++++++++++++++++++++++++++++++++++\n")
            f.write(f"File: {file_path}\n")        
            key_info, total_records = analyze_xml(file_path)
            f.write(f"Total Records: {total_records}\n")
            f.write("Key information with max value lengths, longest values, and null counts:\n")  
            for key, info in sorted(key_info.items()):
                f.write(f"Key: {key}\n")
                f.write(f"Max_Length: {info['max_length']}\n")
                if len(info['longest_value']) > 1000:
                    f.write(f"Long post with more than 1000 chars\n")
                f.write(f"Null count: {info['null_count']}\n")
                f.write(f"Unique Count: {info['unique_count']}\n")
                
            f.write("++++++++++++++++++++++++++++++++++++++++++++++++++++++++\n")
            f.write("\n")

if __name__=="__main__":   
    print('++++++++++++++++++++++++++++++++++++++++++++++')
    print("Exploring Files")
    
    output_file = 'file_info.txt'
    save_results_to_file(input_files, output_file)
    
    print('File Results Stored to ', output_file)
    
    print('++++++++++++++++++++++++++++++++++++++++++++++')
import xml.etree.ElementTree as ET
from code.globals import chunk_size, data_directory, input_files, db_exists
from code.db_utils import *

# This method removes entries with non-existent FK Ids from chunk_data.
def remove_invalid_entries_links(chunk_data, valid_ids, loc):
    # return only data that contains valid ids
    return [data for data in chunk_data if int(data[loc]) in valid_ids]

# This methods removes entries with non-existent FK Ids from chunk_data but ignore entries where the FK Id is None.
def remove_invalid_entries_links_ignore_none(chunk_data, valid_ids, loc):
    # return only data that contains valid ids and None ids
    return [data for data in chunk_data if data[loc] is None or int(data[loc]) in valid_ids]

# This method extract ids from chunk_data on give location to compare FK ids with existing Ids in the database.
def extract_ids_from_chunk(chunk_data, loc):
    # return ids extracted from given location from chunk_data
    return [int(data[loc]) for data in chunk_data]


# This method extract ids from chunk_data on give location to compare FK ids with existing Ids in the database. Only includes non-None values.
def extract_ids_from_chunk_none(chunk_data, loc):
    # # return ids extracted from given location from chunk_data only where is not None
    return [int(data[loc]) for data in chunk_data if data[loc] is not None]

# This method removes entries with non-existent FK Ids from chunk_data.
def remove_invalid_entries_links_chunked(chunk_data, valid_ids, loc, chunk_size=5000):
    # return only data that contains valid ids and None ids
    valid_ids_set = set(valid_ids)
    filtered_data = []
    # Process in chunks
    for i in range(0, len(chunk_data), chunk_size):
        chunk = chunk_data[i:i + chunk_size]
        filtered_data.extend(data for data in chunk if int(data[loc]) in valid_ids_set)
    
    return filtered_data

# This method checks which ids are valid by querying the database and returns only valid_ids to ensure fk is not violated.
def check_valid_fk_ids(table, ids):
    valid_ids = set()
    if ids:
        # Make sure that only to check for valid table names to avoid SQL injection. 
        if table.lower() not in {t.lower() for t in input_files}:
            raise ValueError("Invalid table name")
        sql = "SELECT Id FROM " + table + " WHERE Id IN %s"
        result = exec_get_all(sql, (tuple(ids),))
        valid_ids = [row[0] for row in result]
    return valid_ids

# This method takes an sql script file from root; and executes it
def run_schema_script(file):
    # Run the script file to create schema. 
    exec_sql_file(file)

# This method takes an sql query; and executes it
def run_commit_query(query):
    # Run single query
    exec_commit(query)

# This method inserts users in chunks to database.
# get the input file; make sure the data directory is correct and the input file is present in the data directory;
# otherwise the execution will fail, this is applicable for all insert methods
def insert_users(input_file, query, max_chunks=1):
    input_file = data_directory + input_file
    # get the context for reading the file
    context = ET.iterparse(input_file, events=("start", "end"))

    # an indicator to keep track of chunks; not necessary
    chunk_count = 0
    # an indicator to keep track of elements in the chunck 
    elements_in_chunk = 0
    # List to hold data to be inserted into the database
    chunk_data = []  
    # Iterate over the elements present in the input file
    for event, elem in context:
        if elem.tag == 'row':
            # Collect data from element attributes, handle missing attributes
            if elem.get('Id') is not None:
                element_data = (
                    elem.get('Id'),
                    elem.get('AccountId'),
                    elem.get('DisplayName'),
                    elem.get('AboutMe'),
                    elem.get('CreationDate'),
                    elem.get('Reputation')
                )
                chunk_data.append(element_data)
                elements_in_chunk += 1

            # If chunk is full, save the chunk and reset
            if elements_in_chunk >= chunk_size:
                chunk_count += 1
                # insert into db
                execute_df_values(query, chunk_data)
                chunk_data = []  
                elements_in_chunk = 0

            elem.clear()
    # processing the remaining data in final chunk
    if elements_in_chunk > 0:
        chunk_count += 1
        # insert into db
        execute_df_values(query, chunk_data)

    print(f"Processed {chunk_count} chunks for about {chunk_size} elements.")

# This method use the same structure like insert users; hence omitting the comments
# get the input file; make sure the data directory is correct and the input file is present in the data directory;
# otherwise the execution will fail, this is applicable for all insert methods
def insert_tags(input_file, query, max_chunks=10):
    input_file = data_directory + input_file
    context = ET.iterparse(input_file, events=("start", "end"))
    # take the context, read over the file and insert it
    chunk_count = 0
    elements_in_chunk = 0
    chunk_data = [] 
    for event, elem in context:
        if elem.tag == 'row':
            if elem.get('Id') is not None:
                element_data = (
                    elem.get('Id'),
                    elem.get('TagName')
                )
                chunk_data.append(element_data)
                elements_in_chunk += 1

            # If chunk is full, save the chunk and reset
            if elements_in_chunk >= chunk_size:
                chunk_count += 1
                # insert into db
                execute_df_values(query, chunk_data)
                chunk_data = []  
                elements_in_chunk = 0

            elem.clear()

    # processing the remaining data in final chunk
    if elements_in_chunk > 0:
        chunk_count += 1
        # insert into db
        execute_df_values(query, chunk_data)
    
    print(f"Processed {chunk_count} chunks for about {chunk_size} elements.")

# This method to insert post tags 
# get the input file; make sure the data directory is correct and the input file is present in the data directory;
# otherwise the execution will fail, this is applicable for all insert methods
def insert_post_tags(post_tags_chunk, pt_query):
    # first get all tags as they are very small to avoid excessive database connections
    tags = exec_get_all("select Id, TagName from tags")
    # create map for easier test
    tag_map = {tagname: tag_id for tag_id, tagname in tags}
    processed_chunk_data = []
    for post_id, tags_str in post_tags_chunk:
        if tags_str is not None:
            # clean data to get individual tags
            tag_list = tags_str.replace('<', ' ').replace('>', ' ').split()
            for tag in tag_list:
                # get id for each tag
                tag_id = tag_map.get(tag) 
                if tag_id is not None:
                    # we already know that post id is not null; so add it to chunk data if tag id is also not null
                    processed_chunk_data.append((int(post_id), int(tag_id)))
    # get the FK ids from the chunck; only inserting tags where post and tag ids exist
    post_ids = extract_ids_from_chunk(processed_chunk_data, -2)
    # compare ids with the ids already existing in the primary table for FK ids; provide the primary table; valid_ids are returned as set
    valid_post_ids = check_valid_fk_ids('posts', post_ids)
    # filter out invalid ids; provide the FK index 
    valid_chunk_data = remove_invalid_entries_links(processed_chunk_data, valid_post_ids, -2)
    # get the FK ids from the chunck; only inserting tags where post and tag ids exist
    tag_ids = extract_ids_from_chunk(valid_chunk_data, -1)
    # compare ids with the ids already existing in the primary table for FK ids; provide the primary table; valid_ids are returned as set
    valid_tag_ids = check_valid_fk_ids('tags', tag_ids)
    # get the FK ids from the chunck; provide the FK index
    valid_chunk_data = remove_invalid_entries_links(valid_chunk_data, valid_tag_ids, -1)
    # insert the valid data
    execute_df_values(pt_query, valid_chunk_data)
    

def remove_self_bulk_post(chunk_data, loc):
    # make s set of valid Ids (postIds)
    post_ids = {data[0] for data in chunk_data}
    # make a set of valid Ids based on location 1/3, excluding None
    parent_ids = {data[loc] for data in chunk_data if data[loc] is not None}
    # Find invalid Ids
    invalid_ids = parent_ids - post_ids
    # filter data to remove invalid Ids
    filtered_chunk_data = [data for data in chunk_data if data[loc] not in invalid_ids]
    return filtered_chunk_data

# This method inserts data in a dummy table to ensure that correct data is entered. 
# This is done to avoid memory; as reading whole post has body and tags which can get really big. 
def dummy_posts_insert(input_file):
    import xml.etree.ElementTree as ET

    input_file = data_directory + input_file
    context = ET.iterparse(input_file, events=("start", "end"))

    chunk_data = []
    # read the whole data into chunk 
    for event, elem in context:
        if elem.tag == 'row' and elem.get('Id') is not None and elem.get('OwnerUserId') is not None: 
                # Get posts data to insert into posts
            element_data = (
                    elem.get('Id'), elem.get('ParentId'),
                    elem.get('OwnerUserId'), elem.get('AcceptedAnswerId')
                )
            chunk_data.append(element_data)
            elem.clear()
    # remove posts with invalid parent id
    filter1_parents = remove_self_bulk_post(chunk_data, 1)
    # remove posts with invlaid users
    user_ids = extract_ids_from_chunk(filter1_parents, 2)
    valid_user_ids = check_valid_fk_ids('users', user_ids)
    filter_users = remove_invalid_entries_links_chunked(filter1_parents, valid_user_ids, 2)
    # remove posts with invalid answerid
    filter_answers = remove_self_bulk_post(filter_users, 3)
    # delete table and recreate a dummy 
    dummy_query = "INSERT INTO dummy (Id, ParentId, OwnerUserId, AcceptedAnswerId) VALUES %s"
    # insert data into dummy table 
    execute_df_values(dummy_query, filter_answers)

# This method inserts posts; process posts and post tags chunks togather.
# get the input file; make sure the data directory is correct and the input file is present in the data directory;
# otherwise the execution will fail, this is applicable for all insert methods
def insert_posts(input_file, query, pt_query, max_chunks=5):
    # first do a dummy for safe entry
    dummy_posts_insert(input_file)
    
    input_file = data_directory + input_file
    context = ET.iterparse(input_file, events=("start", "end"))
    
    chunk_count = 0
    elements_in_chunk = 0
    chunk_data = []
    post_tags_chunk = []

    for event, elem in context:
        if elem.tag == 'row':
            if elem.get('Id') is not None and elem.get('OwnerUserId') is not None: 
                # get posts data to insert into posts
                element_data = (
                    elem.get('Id'),elem.get('ParentId'),
                    elem.get('OwnerUserId'),elem.get('AcceptedAnswerId'),
                    elem.get('Title'),elem.get('Body'),
                    elem.get('Score'),elem.get('ViewCount'),
                    elem.get('CreationDate')
                )
                # get tags data to process post tags and then insert into post tags
                tags_data = (
                    elem.get('Id'),
                    elem.get('Tags')
                )
                chunk_data.append(element_data)
                post_tags_chunk.append(tags_data)
                elements_in_chunk += 1

            if elements_in_chunk >= chunk_size:
                chunk_count += 1
                #check valid users for posts
                user_ids = extract_ids_from_chunk(chunk_data, -7)
                valid_user_ids = check_valid_fk_ids('users', user_ids)
                valid_chunk_data = remove_invalid_entries_links(chunk_data, valid_user_ids, -7)
                # check valid parents
                parent_ids = extract_ids_from_chunk_none(valid_chunk_data, -8)
                valid_parent_ids = check_valid_fk_ids('dummy', parent_ids)
                valid_chunk_data = remove_invalid_entries_links_ignore_none(valid_chunk_data, valid_parent_ids, -8)
                # check valid answers
                answer_ids = extract_ids_from_chunk_none(valid_chunk_data, -6)
                valid_answer_ids = check_valid_fk_ids('dummy', answer_ids)
                valid_chunk_data = remove_invalid_entries_links_ignore_none(valid_chunk_data, valid_answer_ids, -6)
                # insert the valid data in posts
                execute_df_values(query, valid_chunk_data)
                # insert post_tags ; post tags processing happens in the insert post tag method
                insert_post_tags(post_tags_chunk, pt_query)
                chunk_data = []
                post_tags_chunk = []
                elements_in_chunk = 0

            elem.clear()

    if elements_in_chunk > 0:
        chunk_count += 1
        # extract valid users ids are added
        user_ids = extract_ids_from_chunk(chunk_data, -7)
        valid_user_ids = check_valid_fk_ids('users', user_ids)
        valid_chunk_data = remove_invalid_entries_links(chunk_data, valid_user_ids, -7)
        # check valid parents
        parent_ids = extract_ids_from_chunk_none(valid_chunk_data, -8)
        valid_parent_ids = check_valid_fk_ids('dummy', parent_ids)
        valid_chunk_data = remove_invalid_entries_links_ignore_none(valid_chunk_data, valid_parent_ids, -8)

        # check valid answers
        answer_ids = extract_ids_from_chunk_none(valid_chunk_data, -6)
        valid_answer_ids = check_valid_fk_ids('dummy', answer_ids)
        valid_chunk_data = remove_invalid_entries_links_ignore_none(valid_chunk_data, valid_answer_ids, -6)
        # insert the valid data in posts
        execute_df_values(query, valid_chunk_data)
        # insert post_tags ; post tags processing happens in the insert post tag method
        insert_post_tags(post_tags_chunk, pt_query)
        print("Creating FK for parent and answer id after insertion")
    
    print(f"Processed {chunk_count} chunks for about {chunk_size} elements.")


# This method checks if Tables are already populated;
def check_db_exists():
    # check if flag is set to true
    if db_exists == True:
        return True
    else:
        return False

def report_db_statistics():
    # loop over all the tables
    for table in input_files:
        # input files are known to avoid SQL injection
        query = "SELECT COUNT(*) FROM " + table + ";"
        result = exec_get_one(query)
        if result:
            print("Table: ", table, " Record Inserted: ", result[0])

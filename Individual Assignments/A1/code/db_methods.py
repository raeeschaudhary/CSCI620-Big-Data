import xml.etree.ElementTree as ET
from code.globals import chunk_size, data_directory, input_files
from code.db_utils import *

def remove_invalid_entries_links(chunk_data, valid_ids, loc):
    """
    Remove entries with non-existent FK Ids from chunk_data.
    """
    return [data for data in chunk_data if int(data[loc]) in valid_ids]

def extract_ids_from_chunk(chunk_data, loc):
    """
    Extract Ids from chunk_data based on the location for the Id in chunk_data. 
    E.g., to compare FK ids with existing Ids in the database. 
    """
    return [int(data[loc]) for data in chunk_data]

def check_valid_fk_ids(table, ids):
    """
    Check which ids are valid by querying the database.
    Returns a set of valid ids.
    """
    valid_ids = set()
    if ids:
        # Make sure that only to check for valid table names to avoid SQL injection. 
        if table.lower() not in {t.lower() for t in input_files}:
            raise ValueError("Invalid table name")
        sql = "SELECT Id FROM " + table + " WHERE Id IN %s"
        result = exec_get_all(sql, (tuple(ids),))
        valid_ids = [row[0] for row in result]
    return valid_ids

def create_schema():
    # Run the script file to create schema. 
    exec_sql_file('create_schema.sql')

def insert_users(input_file, query, max_chunks=1):
    """
    Insert XML file in chunks for database insert. Each insert method uses the same structure; so I will omit comments for the following methods
    :param input_file: Path to the XML file.
    :param query: Number of elements to process in each chunk.
    :param max_chunks: Maximum number of chunks to process.
    """
    # get the input file; make sure the data directory is correct and the input file is present in the data directory;
    # otherwise the execution will fail
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
                    elem.get('AboutMe'),
                    elem.get('AccountId'),
                    elem.get('CreationDate'),
                    elem.get('DisplayName'),
                    elem.get('DownVotes'),
                    elem.get('LastAccessDate'),
                    elem.get('Location'),
                    elem.get('Reputation'),
                    elem.get('UpVotes'),
                    elem.get('Views'),
                    elem.get('WebsiteUrl')
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

# Method for insert badges; follows the same stucture 
def insert_badges(input_file, query, max_chunks=1):
    input_file = data_directory + input_file
    context = ET.iterparse(input_file, events=("start", "end"))
    
    chunk_count = 0
    elements_in_chunk = 0
    chunk_data = []  
    for event, elem in context:
        if elem.tag == 'row':
            if  elem.get('Id') is not None and elem.get('UserId') is not None:
                element_data = (
                        elem.get('Id'),
                        elem.get('Class'),
                        elem.get('Date'),
                        elem.get('Name'),
                        elem.get('TagBased'),
                        elem.get('UserId')
                )
                chunk_data.append(element_data)
                elements_in_chunk += 1

            if elements_in_chunk >= chunk_size:
                chunk_count += 1
                # get the FK ids from the chunck; provide the FK index
                user_ids = extract_ids_from_chunk(chunk_data, -1)
                # compare ids with the ids already existing in the primary table for FK ids; provide the primary table; valid_ids are returned as set
                valid_user_ids = check_valid_fk_ids('users', user_ids)
                # if length of valid_ids and set of Ids is same it means all ids are present in the db. 
                if len(valid_user_ids) == len(set(user_ids)):
                    # Proceed to insert all chuncked data; as it is valid
                    execute_df_values(query, chunk_data)
                    chunk_data = [] 
                    elements_in_chunk = 0
                else:
                    # In case some ids are are invalid (do not link to primary table), filter out invalid ids; provide the FK index 
                    valid_chunk_data = remove_invalid_entries_links(chunk_data, valid_user_ids, -1)
                    # insert the valid data
                    execute_df_values(query, valid_chunk_data)
                    chunk_data = [] 
                    elements_in_chunk = 0
            elem.clear()

    # processing the remaining data in final chunk
    if elements_in_chunk > 0: 
        chunk_count += 1
        # get the FK ids from the chunck; provide the FK index
        user_ids = extract_ids_from_chunk(chunk_data, -1)
        # compare ids with the ids already existing in the primary table for FK ids; provide the primary table; valid_ids are returned as set
        valid_user_ids = check_valid_fk_ids('users', user_ids)
        # if length of valid_ids and set of Ids is same it means all ids are present in the db. 
        if len(valid_user_ids) == len(set(user_ids)):
            # Proceed to insert all chuncked data; as it is valid
            execute_df_values(query, chunk_data)
        else:
            # In case some ids are are invalid (do not link to primary table), filter out invalid ids; provide the FK index 
            valid_chunk_data = remove_invalid_entries_links(chunk_data, valid_user_ids, -1)
            # insert the valid data
            execute_df_values(query, valid_chunk_data)

    print(f"Processed {chunk_count} chunks for about {chunk_size} elements.")

# This method use the same structure like insert badges; hence omitting the comments
def insert_posts(input_file, query, max_chunks=1):
    input_file = data_directory + input_file
    context = ET.iterparse(input_file, events=("start", "end"))
    
    chunk_count = 0
    elements_in_chunk = 0
    chunk_data = [] 

    for event, elem in context:
        if elem.tag == 'row':
            if elem.get('Id') is not None and elem.get('OwnerUserId') is not None:
                element_data = (
                    elem.get('Id'),elem.get('AcceptedAnswerId'),elem.get('AnswerCount'),
                    elem.get('Body'),elem.get('ClosedDate'),elem.get('CommentCount'),
                    elem.get('CommunityOwnedDate'),elem.get('ContentLicense'),
                    elem.get('CreationDate'),elem.get('FavoriteCount'),
                    elem.get('LastActivityDate'),elem.get('LastEditDate'),
                    elem.get('LastEditorDisplayName'),
                    elem.get('LastEditorUserId'),
                    elem.get('OwnerDisplayName'),
                    elem.get('OwnerUserId'),elem.get('ParentId'),
                    elem.get('PostTypeId'),elem.get('Score'),
                    elem.get('Tags'),elem.get('Title'),
                    elem.get('ViewCount')
                )
                chunk_data.append(element_data)
                elements_in_chunk += 1

            if elements_in_chunk >= chunk_size:
                chunk_count += 1
                user_ids = extract_ids_from_chunk(chunk_data, -7)
                valid_user_ids = check_valid_fk_ids('users', user_ids)
                if len(valid_user_ids) == len(set(user_ids)):
                    execute_df_values(query, chunk_data)
                    chunk_data = []  
                    elements_in_chunk = 0
                else:
                    valid_chunk_data = remove_invalid_entries_links(chunk_data, valid_user_ids, -7)
                    execute_df_values(query, valid_chunk_data)
                    chunk_data = []  
                    elements_in_chunk = 0

            elem.clear()

    if elements_in_chunk > 0:
        chunk_count += 1
        user_ids = extract_ids_from_chunk(chunk_data, -7)
        valid_user_ids = check_valid_fk_ids('users', user_ids)
        if len(valid_user_ids) == len(set(user_ids)):
            execute_df_values(query, chunk_data)
        else:
            valid_chunk_data = remove_invalid_entries_links(chunk_data, valid_user_ids, -7)
            execute_df_values(query, valid_chunk_data)

    print(f"Processed {chunk_count} chunks for about {chunk_size} elements.")

# This method use the same structure like insert badges; hence omitting the comments
def insert_post_links(input_file, query, max_chunks=1):
    input_file = data_directory + input_file    
    context = ET.iterparse(input_file, events=("start", "end"))
    
    chunk_count = 0
    elements_in_chunk = 0
    chunk_data = [] 

    for event, elem in context:
        if elem.tag == 'row':
            if elem.get('Id') is not None and elem.get('PostId') is not None and elem.get('RelatedPostId') is not None:
                element_data = (
                    elem.get('Id'),elem.get('CreationDate'),
                    elem.get('LinkTypeId'),elem.get('PostId'),
                    elem.get('RelatedPostId')
                )
                chunk_data.append(element_data)
                elements_in_chunk += 1

            if elements_in_chunk >= chunk_size:
                chunk_count += 1
                post_ids = extract_ids_from_chunk(chunk_data, -2)
                valid_post_ids = check_valid_fk_ids('posts', post_ids)
                related_post_ids = extract_ids_from_chunk(chunk_data, -1)
                valid_related_post_ids = check_valid_fk_ids('posts', related_post_ids)
                
                if len(valid_post_ids) == len(set(post_ids)) and len(valid_related_post_ids) == len(set(related_post_ids)):
                    execute_df_values(query, chunk_data)
                    chunk_data = [] 
                    elements_in_chunk = 0
                else:
                    valid_chunk_data = remove_invalid_entries_links(chunk_data, valid_post_ids, -2)
                    valid_chunk_data = remove_invalid_entries_links(valid_chunk_data, valid_related_post_ids, -1)
                    execute_df_values(query, valid_chunk_data)
                    chunk_data = [] 
                    elements_in_chunk = 0

            elem.clear()

    if elements_in_chunk > 0:
        chunk_count += 1
        post_ids = extract_ids_from_chunk(chunk_data, -2)
        valid_post_ids = check_valid_fk_ids('posts', post_ids)
        related_post_ids = extract_ids_from_chunk(chunk_data, -1)
        valid_related_post_ids = check_valid_fk_ids('posts', related_post_ids)
        if len(valid_post_ids) == len(set(post_ids)) and len(valid_related_post_ids) == len(set(related_post_ids)):
            execute_df_values(query, chunk_data)
        else:
            valid_chunk_data = remove_invalid_entries_links(chunk_data, valid_post_ids, -2)
            valid_chunk_data = remove_invalid_entries_links(valid_chunk_data, valid_related_post_ids, -1)
            execute_df_values(query, valid_chunk_data)

    print(f"Processed {chunk_count} chunks for about {chunk_size} elements.")

# This method use the same structure like insert badges; hence omitting the comments
def insert_comments(input_file, query, max_chunks=1):
    input_file = data_directory + input_file
    context = ET.iterparse(input_file, events=("start", "end"))
    
    chunk_count = 0
    elements_in_chunk = 0
    chunk_data = [] 

    for event, elem in context:
        if elem.tag == 'row':
            if elem.get('Id') is not None and elem.get('PostId') is not None and elem.get('UserId') is not None:
                element_data = (
                    elem.get('Id'),elem.get('ContentLicense'),
                    elem.get('CreationDate'),elem.get('PostId'),
                    elem.get('Score'),elem.get('Text'),
                    elem.get('UserDisplayName'),elem.get('UserId')
                )
                chunk_data.append(element_data)
                elements_in_chunk += 1

            if elements_in_chunk >= chunk_size:
                chunk_count += 1
                post_ids = extract_ids_from_chunk(chunk_data, -5)
                valid_post_ids = check_valid_fk_ids('posts', post_ids)
                user_ids = extract_ids_from_chunk(chunk_data, -1)
                valid_user_ids = check_valid_fk_ids('users', user_ids)
                
                if len(valid_post_ids) == len(set(post_ids)) and len(user_ids) == len(set(valid_user_ids)):
                    execute_df_values(query, chunk_data)
                    chunk_data = []  
                    elements_in_chunk = 0
                else:
                    valid_chunk_data = remove_invalid_entries_links(chunk_data, valid_post_ids, -5)
                    valid_chunk_data = remove_invalid_entries_links(valid_chunk_data, valid_user_ids, -1)
                    execute_df_values(query, valid_chunk_data)
                    chunk_data = []  
                    elements_in_chunk = 0

            elem.clear()

    if elements_in_chunk > 0: 
        chunk_count += 1
        post_ids = extract_ids_from_chunk(chunk_data, -5)
        valid_post_ids = check_valid_fk_ids('posts', post_ids)
        user_ids = extract_ids_from_chunk(chunk_data, -1)
        valid_user_ids = check_valid_fk_ids('users', user_ids)
        if len(valid_post_ids) == len(set(post_ids)) and len(valid_user_ids) == len(set(user_ids)):
            execute_df_values(query, chunk_data)
        else:
            valid_chunk_data = remove_invalid_entries_links(chunk_data, valid_post_ids, -5)
            valid_chunk_data = remove_invalid_entries_links(valid_chunk_data, valid_user_ids, -1)
            execute_df_values(query, valid_chunk_data)

    print(f"Processed {chunk_count} chunks of {chunk_size} elements each.")

# This method use the same structure like insert badges; hence omitting the comments
def insert_post_history(input_file, query, max_chunks=1):
    input_file = data_directory + input_file
    context = ET.iterparse(input_file, events=("start", "end"))
    
    chunk_count = 0
    elements_in_chunk = 0
    chunk_data = []  

    for event, elem in context:
        if elem.tag == 'row':
            if elem.get('Id') is not None and elem.get('PostId') is not None and elem.get('UserId') is not None:
                element_data = (
                    elem.get('Id'),elem.get('Comment'),
                    elem.get('ContentLicense'),elem.get('CreationDate'),
                    elem.get('PostHistoryTypeId'),
                    elem.get('PostId'),elem.get('RevisionGUID'),
                    elem.get('Text'),elem.get('UserDisplayName'),
                    elem.get('UserId')
                )
                chunk_data.append(element_data)
                elements_in_chunk += 1

            if elements_in_chunk >= chunk_size:
                chunk_count += 1
                post_ids = extract_ids_from_chunk(chunk_data, -5)
                valid_post_ids = check_valid_fk_ids('posts', post_ids)
                user_ids = extract_ids_from_chunk(chunk_data, -1)
                valid_user_ids = check_valid_fk_ids('users', user_ids)
                
                if len(valid_post_ids) == len(set(post_ids)) and len(user_ids) == len(set(valid_user_ids)):
                    execute_df_values(query, chunk_data)
                    chunk_data = []  
                    elements_in_chunk = 0
                else:
                    valid_chunk_data = remove_invalid_entries_links(chunk_data, valid_post_ids, -5)
                    valid_chunk_data = remove_invalid_entries_links(valid_chunk_data, valid_user_ids, -1)
                    execute_df_values(query, valid_chunk_data)
                    chunk_data = []  
                    elements_in_chunk = 0

            elem.clear()

    if elements_in_chunk > 0: 
        chunk_count += 1
        post_ids = extract_ids_from_chunk(chunk_data, -5)
        valid_post_ids = check_valid_fk_ids('posts', post_ids)
        user_ids = extract_ids_from_chunk(chunk_data, -1)
        valid_user_ids = check_valid_fk_ids('users', user_ids)
        if len(valid_post_ids) == len(set(post_ids)) and len(valid_user_ids) == len(set(user_ids)):
            execute_df_values(query, chunk_data)
        else:
            valid_chunk_data = remove_invalid_entries_links(chunk_data, valid_post_ids, -5)
            valid_chunk_data = remove_invalid_entries_links(valid_chunk_data, valid_user_ids, -1)
            execute_df_values(query, valid_chunk_data)

    print(f"Processed {chunk_count} chunks of {chunk_size} elements each.")

# This method use the same structure like insert badges; hence omitting the comments
def insert_votes(input_file, query, max_chunks=1):
    input_file = data_directory + input_file
    context = ET.iterparse(input_file, events=("start", "end"))
    
    chunk_count = 0
    elements_in_chunk = 0
    chunk_data = []  

    for event, elem in context:
        if elem.tag == 'row':
            if elem.get('Id') is not None and elem.get('PostId') is not None:
                element_data = (
                    elem.get('Id'),elem.get('BountyAmount'),
                    elem.get('CreationDate'),elem.get('PostId'),
                    elem.get('UserId'),elem.get('VoteTypeId')
                )
                chunk_data.append(element_data)
                elements_in_chunk += 1

            if elements_in_chunk >= chunk_size:
                chunk_count += 1
                post_ids = extract_ids_from_chunk(chunk_data, -3)
                valid_post_ids = check_valid_fk_ids('posts', post_ids)
                
                if len(valid_post_ids) == len(set(post_ids)):
                    execute_df_values(query, chunk_data)
                    chunk_data = [] 
                    elements_in_chunk = 0
                else:
                    valid_chunk_data = remove_invalid_entries_links(chunk_data, valid_post_ids, -3)
                    execute_df_values(query, valid_chunk_data)
                    chunk_data = []  
                    elements_in_chunk = 0

            elem.clear()

    if elements_in_chunk > 0: 
        chunk_count += 1
        post_ids = extract_ids_from_chunk(chunk_data, -3)
        valid_post_ids = check_valid_fk_ids('posts', post_ids)
        if len(valid_post_ids) == len(set(post_ids)):
            execute_df_values(query, chunk_data)
        else:
            valid_chunk_data = remove_invalid_entries_links(chunk_data, valid_post_ids, -3)
            execute_df_values(query, valid_chunk_data)

    print(f"Processed {chunk_count} chunks for about {chunk_size} elements.")

# This method use the same structure like insert badges; hence omitting the comments
def insert_tags(input_file, query, max_chunks=1):
    input_file = data_directory + input_file
    context = ET.iterparse(input_file, events=("start", "end"))
    
    chunk_count = 0
    elements_in_chunk = 0
    chunk_data = [] 
    for event, elem in context:
        if elem.tag == 'row':
            if elem.get('Id') is not None and elem.get('ExcerptPostId') is not None and elem.get('WikiPostId'):
                element_data = (
                    elem.get('Id'),elem.get('Count'),
                    elem.get('ExcerptPostId'),
                    elem.get('TagName'),
                    elem.get('WikiPostId')
                )
                chunk_data.append(element_data)
                elements_in_chunk += 1

            if elements_in_chunk >= chunk_size:
                chunk_count += 1
                post_ids = extract_ids_from_chunk(chunk_data, -3)
                valid_post_ids = check_valid_fk_ids('posts', post_ids)
                wiki_ids = extract_ids_from_chunk(chunk_data, -1)
                valid_wiki_ids = check_valid_fk_ids('posts', wiki_ids)
                
                if len(valid_post_ids) == len(set(post_ids)) and len(wiki_ids) == len(set(valid_wiki_ids)):
                    execute_df_values(query, chunk_data)
                    chunk_data = []  
                    elements_in_chunk = 0
                else:
                    valid_chunk_data = remove_invalid_entries_links(chunk_data, valid_post_ids, -3)
                    valid_chunk_data = remove_invalid_entries_links(valid_chunk_data, valid_wiki_ids, -1)
                    execute_df_values(query, valid_chunk_data)
                    chunk_data = []  
                    elements_in_chunk = 0

            elem.clear()

    if elements_in_chunk > 0: 
        chunk_count += 1
        post_ids = extract_ids_from_chunk(chunk_data, -3)
        valid_post_ids = check_valid_fk_ids('posts', post_ids)
        wiki_ids = extract_ids_from_chunk(chunk_data, -1)
        valid_wiki_ids = check_valid_fk_ids('posts', wiki_ids)
        if len(valid_post_ids) == len(set(post_ids)) and len(valid_wiki_ids) == len(set(wiki_ids)):
            execute_df_values(query, chunk_data)
        else:
            valid_chunk_data = remove_invalid_entries_links(chunk_data, valid_post_ids, -3)
            valid_chunk_data = remove_invalid_entries_links(valid_chunk_data, valid_wiki_ids, -1)
            execute_df_values(query, valid_chunk_data)
    
    print(f"Processed {chunk_count} chunks for about {chunk_size} elements.")

def report_db_statistics():
    # loop over all the tables
    for table in input_files:
        # input files are known to avoid SQL injection
        query = "SELECT COUNT(Id) FROM " + table + ";"
        result = exec_get_one(query)
        if result:
            print("Table: ", table, " Record Inserted: ", result[0])

# Get all comments from the database.
def get_comments():
    return exec_get_all("SELECT * FROM comments;")

# This method test the db state is consistent while failing with an non-existing userId
# This inserts the first row; fails on the second row; the transaction reverses to maintain the db is consistent
def test_transaction():
    try:
        conn = connect()
        conn.autocommit = False
        cur = conn.cursor()
        
        # Insert first row
        print("Inserting Row 1")
        cur.execute("INSERT INTO comments (Id, ContentLicense, CreationDate, PostId, Score, Text, UserDisplayName, UserId) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)",
                    (100000001, 'CC BY-SA 2.5', '2010-07-28T19:36:59.773', 56973, 5, 'Great post!', 'user1', 5))
        print("Row 1 Inserted")
        # Insert second row with error (non-existing UserId). This should fail 
        print("Inserting Row 2")
        cur.execute("INSERT INTO comments (Id, ContentLicense, CreationDate, PostId, Score, Text, UserDisplayName, UserId) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)",
                    (100000002, 'CC BY-SA 2.5', '2010-07-28T19:36:59.773', 56973, 5, 'Another post!', 'user2', -5))
        print("Row 2 Inserted")
        
        # Insert third row
        print("Inserting Row 3")
        cur.execute("INSERT INTO comments (Id, ContentLicense, CreationDate, PostId, Score, Text, UserDisplayName, UserId) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)",
                    (100000003, 'CC BY-SA 2.5', '2010-07-30T19:36:59.773', 56973, 5, 'Nice post!', 'user1', 5))
        print("Row 3 Inserted")            
        
        # Commit transaction
        conn.commit()  
    except Exception as e:
        # Rollback on error
        conn.rollback()  
        print(f"Transaction failed: {e}")
    finally:
        print("Transaction Rollbacked")
        conn.close()
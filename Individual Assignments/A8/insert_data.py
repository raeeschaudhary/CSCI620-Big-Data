import xml.etree.ElementTree as ET
from pymongo import MongoClient
from pymongo.errors import PyMongoError
from globals import chunk_size, data_directory, collections, db_config
import time
from datetime import datetime

def connect():
    """
    Make the connection with the database and return the connection. 
    
    :returns a database client object.
    """
    try:
        # create a mongodb connection using the db_config provided in globals.py
        client = MongoClient(host=db_config['host'], 
                         port=db_config['port'])
        return client[db_config['database']]
    except PyMongoError as e:
        print(f"Error connecting to MongoDB: {e}")
        return None

def fetch_existing_ids(collection, key):
    """
    fetches the existing keys (primary) from an exisitig collection. To get user ids; collection = 'users', key = 'Id'
    
    :param collection: the name of the collection to search keys.
    :param key: the name of the key to fetch data from the collection. 
    :return: dictionary of found data (existing keys) (key, _id).
    """
    # Make a connection
    db = connect()
    # find _id for all values matching with give key (e.g., Id)
    existing_data = db[collection].find({}, {'_id': 1, key: 1})
    # retun key:value mapping for found keys to access _id for each found value
    return {str(doc[key]): doc['_id'] for doc in existing_data}

def filter_and_replace_ids(chunk_data, existing_ids, key):
    """
    filters and replaces existing ids (e.g., UserId = 1) with _id (ObjectId) at given key to establish the foreign key relationship in the chunk data.
    
    :param chunk_data: the document data in chunk to replace key with existing keys in the collection (as FK).
    :param existing_ids: the dictionary of fetched keys from the collection (e.g., valid keys in the collection). 
    :param key: the key to be replaced with _id to make the relationship valid (e.g., UserId).
    :return: valid chunk data that replaced Ids for key with valid existing ids from the collection to be referenced.
    """
    # prepare valid data
    valid_chunk_data = []
    # process each entry in the chunk to replace keys
    for entry in chunk_data:
        # find the key to replace 
        test_id = str(entry.get(key))
        # Ensure to consider test_id is present in chunk data (not none) and valid with existing_ids
        if test_id is not None and str(test_id) in existing_ids:
            # Find and update the key (e.g., UserId) with corresponding _id from the collection; mapped by provided dictionary
            entry[key] = existing_ids[str(test_id)] 
            # add it to valid data
            valid_chunk_data.append(entry)
    # return all valid chunk data by only having entries with valid (existing ids)
    return valid_chunk_data

def cleaning_database():
    """
    clean the database (drop collections) for fresh insertion of the record. 
    """
    # make a database connection
    db = connect()
    # get a list of database connection names
    collections = db.list_collection_names()
    # iterate over collections and drop each and print a message on console.
    for collection_name in collections:
        db[collection_name].drop()
        print(f"Deleted collection: {collection_name}")

def creating_collections():
    """
    Create a list of collections privded by names in the globals collections variable.
    """
    # make a database connection
    db = connect()
    # take the list of collections from globals.py connections
    for collection_name in collections:
        db.create_collection(collection_name)
    # get a list of database connection names
    all_collections = db.list_collection_names()
    # iterate over collections and drop each and print a message on console.
    for collection_name in all_collections:
        print(f"Created collection: {collection_name}")

# This method inserts users in chunks to database.
# get the input file; make sure the data directory is correct and the input file is present in the data directory;
# otherwise the execution will fail, this is applicable for all insert methods
def insert_users(input_file):
    """
    This method inserts users.
    
    :param input_file: Name of the XML file to extract data from.
    """
    # merge the file name with the data_directory provided in globals.py
    input_file = data_directory + input_file
    # get the context for reading the file
    context = ET.iterparse(input_file, events=("start", "end"))
    # connect to the database
    db = connect()
    # get a reference to users collection
    collection = db['users']
    # an indicator to keep track of chunks; not necessary
    chunk_count = 0
    # an indicator to keep track of elements in the chunk 
    elements_in_chunk = 0
    # List to hold data to be inserted into the database
    chunk_data = []  
    # Iterate over the elements present in the input file
    for event, elem in context:
        if event == 'end' and elem.tag == 'row':
            if elem.get('Id') is not None:
                # Collect data from element attributes as document
                document = {
                    "Id": elem.get('Id')
                }
                chunk_data.append(document)
                elements_in_chunk += 1
            # If chunk is full, save the chunk and reset
            if elements_in_chunk >= chunk_size:
                chunk_count += 1
                # from data analysis from previous assignments, the selected fields does not contain nulls, so cleaning is skipped based on that surety.
                # Insert the clean data into db;
                collection.insert_many(chunk_data)
                chunk_data = []  
                elements_in_chunk = 0
            # Clear the element to free up memory
            elem.clear()
        print(f"\rProcessed more than {chunk_count * chunk_size} elements. ", end="")
    
    # Processing the remaining data in final chunk
    if elements_in_chunk > 0:
        chunk_count += 1
        # from data analysis from previous assignments, the selected fields does not contain nulls, so cleaning is skipped based on that surety.
        # Insert the clean data into db;
        collection.insert_many(chunk_data)

def insert_posts(input_file):
    """
    This method insert posts.
    
    :param input_file: Name of the XML file to extract data from.
    """
    # merge the file name with the data_directory provided in globals.py
    input_file = data_directory + input_file
    # get the context for reading the file
    context = ET.iterparse(input_file, events=("start", "end"))
    # connect to the database
    db = connect()
    # get a reference to users collection
    collection = db['posts']
    # first check existing users ids to check for valid entries
    existing_ids = fetch_existing_ids('users', 'Id')
    # an indicator to keep track of chunks; not necessary
    chunk_count = 0
    # an indicator to keep track of elements in the chunk 
    elements_in_chunk = 0
    # List to hold data to be inserted into the database
    chunk_data = []  
    # Iterate over the elements present in the input file
    for event, elem in context:
        if elem.tag == 'row':
            if elem.get('Id') is not None and elem.get('OwnerUserId') is not None and elem.get('ViewCount') is not None and elem.get('Score') is not None: 
                # get tags data to and clean it to remove <, >
                tags_string = elem.get('Tags')
                # insert as tag list
                tag_list = []
                if tags_string:
                    # strip < and > from left and right
                    tags = [tag.strip('>') for tag in tags_string.split('<') if tag.strip()]
                    # create list
                    tag_list = [tag for tag in tags if tag]
                # Collect data from element attributes as document
                document = {
                "Id": elem.get('Id'),
                "OwnerUserId": elem.get('OwnerUserId'),
                "ViewCount": int(elem.get('ViewCount')),
                "Score": int(elem.get('Score')),
                "Tags": tag_list,
                "PostTypeId": int(elem.get('PostTypeId')),
                "CreationDate": datetime.fromisoformat(elem.get('CreationDate')),
                "Title": elem.get('Title'),
                "Body": elem.get('Body'),
                }
                chunk_data.append(document)
                elements_in_chunk += 1

            # If chunk is full, save the chunk and reset
            if elements_in_chunk >= chunk_size:
                chunk_count += 1
                elements_in_chunk = 0
                # filter up the chunk data based on valid existing ids; replace _id with OwnerUserId field
                valid_chunk_data = filter_and_replace_ids(chunk_data, existing_ids, 'OwnerUserId')
                # Insert valid data is not empty; then insert record
                if valid_chunk_data:
                    collection.insert_many(valid_chunk_data)
            
            # Clear the element to free up memory
            elem.clear()
        print(f"\rProcessed more than {chunk_count * chunk_size} elements. ", end="")
    
    # Processing the remaining data in final chunk
    if elements_in_chunk > 0:
        chunk_count += 1
        # filter up the chunk data based on valid existing ids; replace _id with OwnerUserId field
        valid_chunk_data = filter_and_replace_ids(chunk_data, existing_ids, 'OwnerUserId')
        # Insert valid data is not empty; then insert record
        if valid_chunk_data:
            collection.insert_many(valid_chunk_data)

def report_db_statistics():
    """
    This method reports the count of records inserted in collections.
    """
    # connect to the database
    db = connect()
    # get all collections
    collections = db.list_collection_names()
    # print the count of documents in each collection
    for collection_name in collections:
        count = db[collection_name].count_documents({})
        # If the collection is users, count the badges
        if collection_name == "users": 
            print(f"Collection: {collection_name}   Users: {count}")
        if collection_name == "posts":
            print(f"Collection: {collection_name}   Posts: {count}")
            total_tags = 0
            # Iterate through each document in the collection
            for document in db[collection_name].find():
                tags = document.get('Tags', [])
                total_tags += len(tags)
            print(f"Collection: {collection_name}   Tags: {total_tags}")

if __name__=="__main__":
    start_time = time.time()
    
    print('++++++++++++++++++++++++++++++++++++++++++++++')

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

    print("insert posts")
    insert_posts("Posts.xml")
    print("\nposts inserted")
    print('++++++++++++++++++++++++++++++++++++++++++++++')
    
    print('++++++++++++++++++++++++++++++++++++++++++++++')
    print("Report DB Statistics")
    report_db_statistics()
    print('++++++++++++++++++++++++++++++++++++++++++++++')

    end_time = time.time()
    run_time = end_time - start_time
    print("Total running time: ", run_time, " seconds")

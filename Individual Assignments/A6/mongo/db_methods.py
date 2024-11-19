import xml.etree.ElementTree as ET
from pymongo import UpdateOne
from pymongo import MongoClient
from pymongo.errors import PyMongoError
import pandas as pd
import matplotlib.pyplot as plt
from datetime import datetime
import json
from globals import data_directory, db_config, chunk_size

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
    fetches the existing keys (primary) from an existing collection. To get user ids; collection = 'users', key = 'Id'
    
    :param collection: the name of the collection to search keys.
    :param key: the name of the key to fetch data from the collection. 
    :return: dictionary of found data (existing keys) (key, _id).
    """
    # Make a connection
    db = connect()
    # find _id for all values matching with give key (e.g., Id)
    existing_data = db[collection].find({key: {"$exists": True}}, {'_id': 1, key: 1})
    # retun key:value mapping for found keys to access _id for each found value
    return {str(doc[key]): doc['_id'] for doc in existing_data}

def filter_and_replace_ids_chunk(chunk_data, existing_ids, key, swap_key=False):
    """
    filters and replaces existing ids (e.g., UserId = 1) with _id (ObjectId) at in the chunk data in cases of updating existing collection.
    
    :param chunk_data: the list of tuples in chunk (id: document) to replace key with existing keys in the collection (as FK).
    :param existing_ids: the dictionary of fetched keys from the collection (e.g., valid keys in the collection). 
    :param key: the key to be replaced with _id to make the relationship valid (e.g., UserId).
    :param swap_key: True or False to swap the key value with _id to map to entry in the same collection.
    :return: valid chunk data that replaced Ids for key with valid existing ids from the collection to be referenced.
    """
    # prepare valid data
    valid_chunk_data = []
    # process each entry in the chunk to replace keys; now chunk is key: document (userId: Achievement)
    for _id, sub_chunk_data in chunk_data:
        # Get the key to replace (UserId) from chunk_data
        test_id = str(sub_chunk_data.get(key))
        # Ensure to consider test_id is present in chunk data (not none) and valid with existing_ids
        if test_id is not None and str(test_id) in existing_ids:
            # Find and update the key (e.g., UserId) with corresponding _id from the collection; mapped by provided dictionary
            sub_chunk_data[key] = existing_ids[str(test_id)]
            # replace the existing _id with object ID to match insertion; if swap_key==True
            if swap_key and str(_id) in existing_ids:
                _id = existing_ids[str(_id)]
            # add it to valid data; keep the tuple formation
            valid_chunk_data.append((_id, sub_chunk_data))
    # return all valid chunk data by only having entries with valid (existing ids)
    return valid_chunk_data

def filter_and_replace_ids_chunk_single(chunk_data, existing_ids):
    """
    filters and replaces existing IDs (e.g., UserId = 1) with _id (ObjectId) in the chunk data for updating a specific field in the collection.
    
    :param chunk_data: the list of tuples in chunk (id: document) to replace key with existing keys in the collection (as FK).
    :param existing_ids: the dictionary of fetched keys from the collection (e.g., valid keys in the collection). 
    :param key: the key to be replaced with _id to make the relationship valid (e.g., UserId).
    :return: valid chunk data that replaced Ids for key with valid existing ids from the collection to be referenced.
    """
    # Prepare valid data
    valid_chunk_data = []
    
    # Process each entry in the chunk to replace keys
    for _id, account_id in chunk_data:
        # Check if the user ID exists in existing_ids
        if str(_id) in existing_ids:
            # Get the corresponding ObjectId from existing_ids
            object_id = existing_ids[str(_id)]
            # Add the valid tuple (object_id, badge_id) to valid_chunk_data
            valid_chunk_data.append((object_id, account_id))
    
    # Return valid chunk data containing only entries with valid (existing IDs)
    return valid_chunk_data


def bulk_update_collection_with_single_field(chunk_data, collection_name, field_name):
    """
    This method updates the collection with a single field insertion or update.
    
    :param chunk_data: the chunk of data containing tuples of main id and field value to set.
    :param collection_name: the name of the collection in the database to update.
    :param field_name: the name of the field in the collection to update or insert.
    """
    # Connect to the database
    db = connect()
    # Get a reference to the collection
    collection = db[collection_name]
    # Create a list to hold all update operations
    bulk_operations = []
    # counter to keep record of updates
    counter = 0
    # Process each chunk as a tuple pair of main id (where to update) and field value (what to update)
    for main_id, field_value in chunk_data:
        # print(main_id, field_value)
        # Update using the _id, which is already replaced in chunk data, Don't insert if the match doesn't exist
        operation = UpdateOne(
            {'_id': main_id},
            {'$set': {field_name: field_value}},
            upsert=False  # Set to True if you want to insert if not found
        )
        bulk_operations.append(operation)
        counter += 1

    try:
        # Execute all operations in a single bulk call
        collection.bulk_write(bulk_operations)
        return counter
    except Exception as e:
        print(f"Error in bulk update: {e}")
        return 0

def bulk_update_collection_with_subset(chunk_data, collection_name, subset_name):
    """
    This method updates the collection with sub-document insertion or update.
    
    :param chunk_data: the chunk of data containing subset documents read and mapped from the input file.
    :param collection_name: the name of the collection in the databse to update.
    :param subset_name: the name of sub-document (array) in the collection to update.
    """
    # connect to the database
    db = connect()
    # get a reference to the collection
    collection = db[collection_name]
    # Create a list to hold all update operations
    bulk_operations = []
    # counter to keep record of updates
    counter  = 0
    # process each chunk as tuple pair of main id (where to update) and sub data (what to update)
    for main_id, sub_data in chunk_data:
        # Update using the _id, which is already replaced in chunk data, Don't insert if the match doesn't exist
        operation = UpdateOne(
            {'_id': main_id}, 
            {'$addToSet': {subset_name: sub_data}},
            upsert=False
        )
        bulk_operations.append(operation)
        counter += 1
    try:
        # Execute all operations in a single bulk call
        collection.bulk_write(bulk_operations)
        return counter
    # throw an error if there an an issue.
    except Exception as e:
        print(f"Error in bulk update: {e}")
        return 0

def parse_account_data(input_file, main_key, filter_key = None, filter_value = None, filter=False):
    """
    This method parses new json data based input file using a main key and other filters to test matching. 
    
    :param input_file: Name of the JSON file to extract data from.
    :param main_key: the key used to map with the existing collection (e.g., account_id).
    :param filter_key: the key used to filter data from the json file (e.g., account_id).
    :param filter_value: the key value used to filter data from the json file (e.g., account_id).
    :param filter: the condition to apply or not apply the filter.
    """
    # collect account data 
    account_site_data = []
    # read file and process data 
    with open(input_file, 'r', encoding='utf-8') as f:
        for line in f:
            # load a line
            data = json.loads(line)
            # if no filter is applied
            if filter == False:
                # get the account_id to map records
                main_map_id = data.get(main_key)
                # get the site name to remove duplicate sites for each account (if any)
                site_name = data.get('site_name')
                # Check if the site already exists in the list for an account data
                existing_sites = {site[1]['site_name'] for site in account_site_data if site[0] == main_map_id}
                # add site data if it is not already added
                if site_name not in existing_sites:
                    site_data = {
                        "main_map_id": main_map_id,
                        "site_name": site_name,
                        "reputation": data.get("reputation"),
                        "badge_counts": data.get("badge_counts"),
                        # fields included for testing matching based on question description
                        "last_access_date": datetime.fromtimestamp(data.get("last_access_date")),
                        "creation_date": datetime.fromtimestamp(data.get("creation_date")),
                        "user_id": data.get("user_id"),
                        # fields that may not be necessary to include based on question description
                        "question_count": data.get("question_count"),
                        "answer_count": data.get("answer_count")
                    }
                    # Add site data to the sites list if it's unique
                    account_site_data.append((main_map_id, site_data))
            else:
                # do the same process as above after applying filter 
                extractd_filter = data.get(filter_key)
                if extractd_filter == filter_value:
                    # get the account_id to map records
                    main_map_id = data.get(main_key)
                    # get the site name to remove duplicate sites for each account (if any)
                    site_name = data.get('site_name')
                    # Check if the site already exists in the list for an account data
                    existing_sites = {site[1]['site_name'] for site in account_site_data if site[0] == main_map_id}
                    # add site data if it is not already added
                    if site_name not in existing_sites:
                        site_data = {
                            "main_map_id": main_map_id,
                            "site_name": site_name,
                            "reputation": data.get("reputation"),
                            "badge_counts": data.get("badge_counts"),
                            # fields included for testing matching based on question description
                            "last_access_date": datetime.fromtimestamp(data.get("last_access_date")),
                            "creation_date": datetime.fromtimestamp(data.get("creation_date")),
                            "user_id": data.get("user_id"),
                            # fields that may not be necessary to include based on question description
                            "question_count": data.get("question_count"),
                            "answer_count": data.get("answer_count")
                        }
                        # Add site data to the sites list if it's unique
                        account_site_data.append((main_map_id, site_data))
        # return the cleaned site data for each account id
    return account_site_data

def insert_accountid_users(input_file):
    """
    This method insert account id in users collection.
    
    :param input_file: Name of the XML file to extract data from.
    """
    # merge the file name with the data_directory provided in globals.py
    input_file = data_directory + input_file
    # first check existing_user_ids to check for valid entries
    existing_user_ids = fetch_existing_ids('users', 'Id')
    # get the context for reading the file
    context = ET.iterparse(input_file, events=("start", "end"))
    # an indicator to keep track of chunks; not necessary
    chunk_count = 0
    # an indicator to keep track of elements in the chunk 
    elements_in_chunk = 0
    # updates count
    counter = 0
    # List to hold data to be inserted into the database
    chunk_data = []  
    for event, elem in context:
        if event == 'end' and elem.tag == 'row':
            if elem.get('Id') is not None:
                # Collect data from element attributes as document
                user_id = elem.get('Id')
                account_id = elem.get('AccountId')
                # collect comp id as key for each competition entry
                chunk_data.append((user_id, account_id))
                elements_in_chunk += 1
            # If chunk is full, save the chunk and reset
            if elements_in_chunk >= chunk_size:
                chunk_count += 1
                # filter up the chunk data based on valid existing_user_ids; to chunk_data insert valid account_ids
                valid_chunk_data = filter_and_replace_ids_chunk_single(chunk_data, existing_user_ids)
                # update bulk of account id with users
                if valid_chunk_data:
                    counter += bulk_update_collection_with_single_field(valid_chunk_data, 'users', 'AccountId')
                chunk_data = []  
                elements_in_chunk = 0
            # Clear the element to free up memory
            elem.clear()
        print(f"\rProcessed more than {chunk_count * chunk_size} elements. ", end="")
        # if chunk_count > 2: break
    # Processing the remaining data in final chunk
    if elements_in_chunk > 0:
        chunk_count += 1
        # filter up the chunk data based on valid existing ids; replace _id with UserId field
        valid_chunk_data = filter_and_replace_ids_chunk_single(valid_chunk_data, existing_user_ids)
        if valid_chunk_data:
            counter += bulk_update_collection_with_single_field(valid_chunk_data, 'users', 'AccountId')
    print(f"\n Records updated: {counter}")

def insert_sites_data_users(input_file):
    """
    This method insert sites data in users collection.
    
    :param input_file: Name of the XML file to extract data from.
    """
    # merge the file name with the data_directory provided in globals.py
    input_file = data_directory + input_file
    # get extra data from the provided file based on account_id
    accounts_data = parse_account_data(input_file, 'account_id')
    # get a reference to users collection with account ID
    existing_ids = fetch_existing_ids('users', 'AccountId')
    # counter updates
    counter = 0
    # check valid account data against check with valid account ids present in the users collection, replace it with the account_id with users _id to make Sites insertion 
    valid_account_data = filter_and_replace_ids_chunk(accounts_data, existing_ids, 'main_map_id', True)
    # if account data is present; update the valid data.
    if valid_account_data:
        counter += bulk_update_collection_with_subset(valid_account_data, 'users', 'Sites')
        valid_account_data = []
    # report how many reocrds updated
    print(f"Records updated: {counter}")

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
            total_badges = 0
            total_sites = 0
            # Iterate through each document in the collection
            for document in db[collection_name].find():
                badges = document.get('Badges', [])
                sites = document.get('Sites', [])
                total_badges += len(badges)
                total_sites += len(sites)
            print(f"Collection: {collection_name}   Badges: {total_badges}")
            print(f"Collection: {collection_name}   Sites: {total_sites}")
        # If the collection is posts, count the comments
        if collection_name == "posts":
            print(f"Collection: {collection_name}   Posts: {count}")
            total_comments = 0
            total_tags = 0
            # Iterate through each document in the collection
            for document in db[collection_name].find():
                tags = document.get('Tags', [])
                comments = document.get('Comments', [])
                total_tags += len(tags)
                total_comments += len(comments)
            print(f"Collection: {collection_name}   Comments: {total_comments}")
            print(f"Collection: {collection_name}   Tags: {total_tags}")


def perform_matching_and_report(input_file):
    """
    This method checks on matching different attributes and report the possible count of matches in users collection.
    
    :param input_file: Name of the XML file to extract data from.
    """
    # merge the file name with the data_directory provided in globals.py
    input_file = data_directory + input_file
    print("++++++++++++++++++++++++++++++++++++++++++++++++++++++++++")
    print("First using the matching considering account ID is present")
    # get extra data from the provided file based on account_id
    accounts_data = parse_account_data(input_file, 'account_id')
    # get a reference to users collection with account ID
    existing_ids = fetch_existing_ids('users', 'AccountId')
    # check valid account data against check with valid account ids present in the users collection, replace it with the account_id with users _id to make Sites insertion 
    valid_account_data = filter_and_replace_ids_chunk(accounts_data, existing_ids, 'main_map_id', True)
    print(f"Records matched: {len(valid_account_data)}")

    print("++++++++++++++++++++++++++++++++++++++++++++++++++++++++++")
    print("NOW CONSIDERING THERE IS NO ACCOUNT ID")
    print("Matching on users Id")
    # get extra data from the provided file based on user_id
    accounts_data = parse_account_data(input_file, 'user_id')
    # get a reference to users collection with ID
    existing_ids = fetch_existing_ids('users', 'Id')
    # check valid account data against check with valid user ids present in the users collection, 
    valid_account_data = filter_and_replace_ids_chunk(accounts_data, existing_ids, 'main_map_id', True)
    print(f"Records matched: {len(valid_account_data)}")
    
    print("++++++++++++++++++++++++++++++++++++++++++++++++++++++++++")
    print("Matching on site_name: Ask Ubuntu")
    # get extra data from the provided file based on user_id for site_name
    accounts_data = parse_account_data(input_file, 'user_id', 'site_name', 'Ask Ubuntu', True)
    # get a reference to users collection with ID
    existing_ids = fetch_existing_ids('users', 'Id')
    # check valid account data against check with valid user ids present in the users collection, 
    valid_account_data = filter_and_replace_ids_chunk(accounts_data, existing_ids, 'main_map_id', True)
    print(f"Records matched: {len(valid_account_data)}")
    
    print("++++++++++++++++++++++++++++++++++++++++++++++++++++++++++")
    print("Matching on site_url: https://askubuntu.com")
    # get extra data from the provided file based on user_id for site_url
    accounts_data = parse_account_data(input_file, 'user_id', 'site_url', 'https://askubuntu.com', True)
    # get a reference to users collection with ID
    existing_ids = fetch_existing_ids('users', 'Id')
    # check valid account data against check with valid user ids present in the users collection, 
    valid_account_data = filter_and_replace_ids_chunk(accounts_data, existing_ids, 'main_map_id', True)
    print(f"Records matched: {len(valid_account_data)}")

    print("++++++++++++++++++++++++++++++++++++++++++++++++++++++++++")
    print("Matching on users creation_date")
    # get extra data from the provided file based on creation_date
    accounts_data = parse_account_data(input_file, 'creation_date')
    # get a reference to users collection with CreationDate
    existing_ids = fetch_existing_ids('users', 'CreationDate')
    # check valid account data against check with valid CreationDate ids present in the users collection, 
    valid_account_data = filter_and_replace_ids_chunk(accounts_data, existing_ids, 'main_map_id', True)
    print(f"Records matched: {len(valid_account_data)}")

    print("++++++++++++++++++++++++++++++++++++++++++++++++++++++++++")
    print("Matching on users last_access_date")
    # get extra data from the provided file based on last_access_date
    accounts_data = parse_account_data(input_file, 'last_access_date')
    # get a reference to users collection with account ID
    existing_ids = fetch_existing_ids('users', 'LastAccessDate')
    # check valid account data against check with valid LastAccessDate ids present in the users collection, 
    valid_account_data = filter_and_replace_ids_chunk(accounts_data, existing_ids, 'main_map_id', True)
    print(f"Records matched: {len(valid_account_data)}")

def report_reputation_summary():
    """
    This method reports five-number summary for the reputation of users.
    """
    # connect to the database
    db = connect()
    # get a reference to the users collection
    collection = db['users']
    # Take rep_data as storing all reputation values
    rep_data = []
    # Fetch sites.reputation and iterate over each site to get reputations
    for document in collection.find({}, {"Sites.reputation": 1, "_id": 0}):
        for site in document.get("Sites", []):
            if "reputation" in site:
                # Add reputation for each site
                rep_data.append(site["reputation"])
    # Convert it to pandas DataFrame
    rep_data = pd.DataFrame(rep_data, columns=["Reputation"])
    # Generate five-number summary
    summary = rep_data["Reputation"].describe(percentiles=[0.25, 0.5, 0.75])
    # print the results to console
    print(summary)
    # Create a boxplot pyplot plotting
    plt.figure(figsize=(10, 6))
    plt.boxplot(rep_data["Reputation"])
    plt.title("Five-Number Summary of User Reputation")
    plt.xlabel("Reputation")
    # Save the figure 
    plt.savefig("q4_1_plot.png")

def report_avg_questions_top10_tags():
    """
    This method reports average questions for top 10 tags as bar chart.
    """
    # connect to the database
    db = connect()
    # get a reference to the posts collection
    collection = db['posts']
    # Take questions_data with tags and PostTypeId = 1 (questions)
    questions_data = []
    # Fetch PostTypeId and fetch tags
    for document in collection.find({"PostTypeId": 1}, {"Tags": 1, "_id": 0}):
        tags = document.get("Tags", [])
        # fetch if tags are present 
        if isinstance(tags, list):
            questions_data.extend(tags)
    # count the occurrences of each tag using pandas series
    tag_counts = pd.Series(questions_data).value_counts()
    # select the top 10 most-used tags
    top_tags = tag_counts.head(10)
    # calculate the total questions 
    total_questions = len(collection.find({"PostTypeId": 1}).distinct("Id"))
    # get average questions for top tags
    avg_ques_per_tag = top_tags / total_questions
    # print the results to console
    print(avg_ques_per_tag)
    # Create a bargraph pyplot plotting
    plt.figure(figsize=(12, 6))
    avg_ques_per_tag.plot(kind="bar")
    plt.title("Average Question Count for Tags (Top 10) Across All Posts")
    # plot is cut from bottom, so rotating x-labels
    plt.xticks(rotation=0)
    plt.xlabel("Tag Name")
    plt.ylabel("Avg Question Count")
    # Save the figure 
    plt.savefig("q4_2_plot.png")

def report_yearly_questions():
    """
    This method reports yearly questions asked as timeseries plot.
    """
    # connect to the database
    db = connect()
    # get a reference to the posts collection
    collection = db['posts']
    # Take questions_data with CreationDate and PostTypeId = 1 (questions)
    questions_data = []
    # Fetch PostTypeId and fetch CreationDate
    for document in collection.find({"PostTypeId": 1}, {"CreationDate": 1, "_id": 0}):
        # get year from creation date
        creation_year = document.get("CreationDate").year
        questions_data.append(creation_year)
    # count the occurrences of each year wise data and convert to pandas
    yearly_questions = pd.DataFrame(questions_data, columns=["Year"])
    # Count the number of questions asked each year
    yearly_counts = yearly_questions["Year"].value_counts().sort_index()
    # print the results to console
    print(yearly_counts)
    # Create a bargraph pyplot plotting
    plt.figure(figsize=(12, 6))
    plt.plot(yearly_counts.index, yearly_counts.values)
    plt.title("Count of Questions Asked Each Year")
    plt.xlabel("Year")
    plt.ylabel("Question Count")
    # Save the figure 
    plt.savefig("q4_3_plot.png")
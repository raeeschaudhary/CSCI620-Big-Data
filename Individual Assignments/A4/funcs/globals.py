# Provide the db credentials for your machine
db_config = {
    'host': 'localhost',
    'port': 27017,
    'database': 'ubuntu4'
}
# provide the dataset folder; make sure to include the last slash(es).
# data_directory = 'C:\\Users\\Muhammad Raees\\OneDrive - rit.edu\\Python_Projects\\BigData\\data\\'
data_directory = 'C:\\Users\\mr2714\\OneDrive - rit.edu\\Python_Projects\\BigData\\data\\'

# this is static; increasing uses more memory but less connections to db; reducing make more connection to db
chunk_size = 100000

# these are the list of collections to be used for storing documents
collections = ['users', 'posts']
# these are the list of tables in the db; I use it check table results; to avoid sql injection
input_files = ["users", "badges", "posts", "comments"]

# get the database connection
from pymongo import MongoClient
from pymongo.errors import PyMongoError

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

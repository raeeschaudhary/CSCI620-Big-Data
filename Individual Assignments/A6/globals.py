# Provide the db credentials for your machine
db_config = {
    'host': 'localhost',
    'port': 27017,
    'database': 'ubuntu5'
}
# for this assignment we assume that the DB is populated. If not, please run the assignment 4 first.
db_exist = True

# provide the dataset folder; make sure to include the last slash(es).
# data_directory = 'C:\\Users\\Muhammad Raees\\OneDrive - rit.edu\\Python_Projects\\BigData\\small_data\\'
data_directory = 'C:\\Users\\mr2714\\OneDrive - rit.edu\\Python_Projects\\BigData\\data\\'

# this is static; increasing uses more memory but less connections to db; reducing make more connection to db
chunk_size = 50000

# these are the list of collections to be used for storing documents
collections = ['users', 'posts']
# these are the list of tables in the db; I use it check table results; to avoid sql injection
input_files = ["users", "badges", "posts", "comments"]
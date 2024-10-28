# Provide the db credentials for your machine
db_config = {
    'host': 'localhost',
    'database': 'ubuntu3',
    'user': 'postgres',
    'password': 'root',
    'port': '5432'
}
# change it to true if database already is populated with the needed table to create new relation
db_exists = True
# provide the dataset folder; make sure to include the last slash(es).
# data_directory = 'C:\\Users\\Muhammad Raees\\OneDrive - rit.edu\\Python_Projects\\BigData\\data\\'
data_directory = 'C:\\Users\\mr2714\\OneDrive - rit.edu\\Python_Projects\\BigData\\data\\'

# this is static; increasing uses more memory but less connections to db; reducing make more connection to db
chunk_size = 10000

# these are the list of tables in the db; I use it check table results; to avoid sql injection
input_files = ["users", "tags", "posts", "posttags", "dummy"]
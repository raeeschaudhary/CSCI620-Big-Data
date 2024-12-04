# Provide the db credentials for your machine
db_config = {
    'host': 'localhost',
    'port': 27017,
    'database': 'ubuntu8'
}
# QUESTION 2. QUESTION 3. set K and T; K=samples, T=tag; make sure its a valid tag from subset_tags given below
K = 5
T = 'boot'

# subset of tags
subset_tags = ['command-line', 'boot', 'networking', 'apt', 'drivers']
# best Ks after running Question 4, sequenced as subset_tags 
# command-line = 20, boot = 20, networking = 25, apt = 15, drivers = 15 
best_Ks = [20, 20, 25, 15, 15]
# provide the dataset folder; make sure to include the last slash(es).
# data_directory = 'C:\\Users\\Muhammad Raees\\OneDrive - rit.edu\\Python_Projects\\BigData\\small_data\\'
data_directory = 'C:\\Users\\mr2714\\OneDrive - rit.edu\\Python_Projects\\BigData\\data\\'

# this is static; increasing uses more memory but less connections to db; reducing make more connection to db
chunk_size = 71234

# these are the list of collections to be used for storing documents
collections = ['users', 'posts', 'kmposts', 'centroids']

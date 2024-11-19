# Provide the db credentials for your machine
db_config = {
    'host': 'localhost',
    'database': 'ubuntu5',
    'user': 'postgres',
    'password': 'root',
    'port': '5432'
}
# for this assignment we assume that the DB is populated. If not, please run the assignment 2 first.
db_exist = True

# these are the list of tables in the db; I use it check table results; to avoid sql injection
input_files = ["users", "badges", "tags", "posts", "posttags", "comments"]
# sources
non_m_source_views = ['NetworkingPosts', 'NonNetworkingPosts', 'NetworkingUsers', 'NonNetworkingUsers', 'CommentedOn']
m_source_views = ['NetworkingPostsMaterialized', 'NonNetworkingPostsMaterialized', 'NetworkingUsersMaterialized', 'NonNetworkingUsersMaterialized', 'CommentedOnMaterialized']

# gavs
non_m_gavs =['AllPosts', 'AllUsers', 'AllComments']
m_gavs = ['AllPostsMaterialized', 'AllUsersMaterialized', 'AllCommentsMaterialized']
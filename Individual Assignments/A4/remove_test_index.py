from funcs.queries import *
import pprint
import time


def delete_index(collection_name, index_name):
    """
    Delete an index from the specified collection by name.
    
    Parameters:
    - collection_name: The name of the collection
    - index_name: The name of the index to delete
    """
    db = connect()
    collection = db[collection_name]
    
    try:
        collection.drop_index(index_name)
        print(f"Index '{index_name}' removed successfully from '{collection_name}'.")
    except Exception as e:
        print(f"Error removing index '{index_name}' from '{collection_name}': {e}")


if __name__=="__main__":
    start_time = time.time()
    # Explaining Query 1
    list_index1 = [
        'index_display_name', 
        'index_user_creation_date',
        'index_badges_name',
        'index_badges_date',
    ]
    list_index2 = [
    'index_post_owner_id',
        'index_post_type_id',
        'index_post_creation_date',
        'index_post_score',
        'index_post_tags',
        'index_comments_user_id',
        'index_comments_creation_date',
        'index_comments_score',
    ]
    for idx in list_index1:
        delete_index('users', idx)
        
    for idx in list_index2:
        delete_index('posts', idx)
    print('++++++++++++++++++++++++++++++++++++++++++++++')
    print('delete index')
    delete_index('posts', 'CreationDate_1')
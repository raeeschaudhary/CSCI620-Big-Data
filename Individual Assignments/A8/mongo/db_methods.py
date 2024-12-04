from pymongo import UpdateOne
from pymongo import MongoClient
from pymongo.errors import PyMongoError
import matplotlib.pyplot as plt
import math
from globals import db_config

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

def drop_collection(collection_name):
    """
    clean the collection (drop collection) for fresh subset of the record. 

    :params collection_name: the input name of the collection to clean. 
    """
    # make a database connection
    db = connect()
    # get a list of database connection names
    collections = db.list_collection_names()
    # iterate over collections and drop the given collection and print a message on console.
    for coll_name in collections:
        if coll_name == collection_name:
            db[coll_name].drop()
            print(f"{coll_name} collection deleted. Starting Fresh.")

def create_subset_posts(collection_name, subset_tags):
    """
    This method create a new collection with subset of tags.
    
    :params collection_name: the input name of the collection to delete and create clean. 
    :params subset_tags: the subset of tags provided as array to match from. 
    """
    # first clean up the collection, 'kmposts'
    drop_collection(collection_name)
    # connect to the database
    db = connect()
    # take the posts collection to read data from
    posts_collection = db['posts']
    posts_subset_query = [
        # first match documents with subset tags
        { "$match": {"Tags": {"$in": subset_tags}} },
        # then remove duplicates based on the unique _id field
        {
            "$group": {
                "_id": "$_id",
                "document": {"$first": "$$ROOT"}
            }
        },
        # keep the whole document (all fields)
        {
            "$replaceRoot": {"newRoot": "$document"}
        }
    ]
    # take the collection 'kmposts' to write data as subset with matching tags
    kmposts_collection = db[collection_name]
    # first get the posts, allowDiskUse to handle data if it is more than memory limit
    subset_posts = posts_collection.aggregate(posts_subset_query, allowDiskUse=True)
    # then insert the posts
    if subset_posts:
        kmposts_collection.insert_many(subset_posts)

def normalize_kmposts(collection_name):
    """
    This method updates the collecton with normalized values for view count and score. 
    
    :params collection_name: the input name of the collection to update. 
    """
    # connect to the database
    db = connect()
    # take the collection_name 'kmposts' to read data from
    kmposts_collection = db[collection_name]
    # find the posts, get their view count and score
    kmposts = list(kmposts_collection.find({}, {"_id": 1, "ViewCount": 1, "Score": 1}))
    # bulk update collection by adding the subset data
    view_counts = [int(doc["ViewCount"]) for doc in kmposts if "ViewCount" in doc]
    # extract score for normalization
    scores = [int(doc["Score"]) for doc in kmposts if "Score" in doc]
    # get the min and max for view counts and score
    min_view_count = min(view_counts)
    max_view_count = max(view_counts)
    min_score = min(scores)
    max_score = max(scores)
    # create a list to hold all update operations
    bulk_operations = []
    # process each doc from posts as doc to update
    for doc in kmposts:
        # get the view count of each kmpost and score for each doc
        view_count = int(doc["ViewCount"])
        score = int(doc["Score"])
        # compute normalized values
        norm_view_count = (view_count - min_view_count) / (max_view_count - min_view_count)
        norm_score = (score - min_score) / (max_score - min_score)
        # assign values in kmeansNorm array
        kmeans_norm = [norm_view_count, norm_score]
        # update using the _id, don't insert if the match doesn't exist
        operation = UpdateOne(
                {"_id": doc["_id"]},
                {"$set": {"kmeansNorm": kmeans_norm}},
                upsert=False
            )
        bulk_operations.append(operation)
    try:
        # Execute all operations in a single bulk call
        kmposts_collection.bulk_write(bulk_operations)
    # throw an error if there an an issue.
    except Exception as e:
        print(f"Error in bulk update: {e}")
    
def create_centroids(sample_size, tag_name):
    """
    This method create a collection centriods, using K centroids for T tag.
    
    :params sample_size: the number of samples (count) for centroids. 
    :params tag_name: the name of the tag to take samples from kmposts. 
    """
    # first clean up the collection, 'centroids'
    drop_collection('centroids')
    # connect to the database
    db = connect()
    # take the posts collection to read data from
    posts_collection = db["kmposts"]
    # take the kmposts to write data as subset with matching tags
    centroid_collection = db['centroids']
    # set centroid for each K starting from 1
    centeroid = 1
    # get a sample from tag, K, T set in globals.py
    cluster_query = [
        # first match documents with a tag
        { "$match": {"Tags": {"$in": [tag_name]}} },
        # then select sample of size K, set in globals.py
        {"$sample": {"size": sample_size}}
    ]
    # first get the sample posts based on the tag
    kmposts = list(posts_collection.aggregate(cluster_query))
    # create a list to hold all insert operations
    bulk_operations = []
    for doc in kmposts:
        # insert documents from 1 to K, set PK as cluster/centroid number, only take kmeansNorms
        operation = {
            "_id": centeroid,
            "kmeansNorm": doc["kmeansNorm"]
        }
        bulk_operations.append(operation)
        # move to next sample to give centroid name, i to K
        centeroid += 1
    # insert into centroids collection, 1...K
    if bulk_operations:
        centroid_collection.insert_many(bulk_operations)

def update_tag_cluster(tag_posts, centroids):
    """
    This method updates the cluster value based on centriods, for T tag.
    
    :params tag_posts: the posts to find clusters based on centroids. 
    :params centroids: the centroids to find distances between those and posts. 
    :returns updated tag posts with clusters assigned to closest centroid and the sum of squared error. 
    """
    # variable to keep track of sum of squaured error, set to 0
    sse = 0
    # get centroid norms with ids for cacluation
    centroid_norms = {c["_id"]: c["kmeansNorm"] for c in centroids}
    # iterate over all selected posts to update clusters based on distances
    for doc in tag_posts:
        # get the doc norm for calculation
        doc_norm = doc["kmeansNorm"]
        # set the min_distance as very large
        min_distance = float("inf")
        # set the closest centroid to 0, as there is no 0 cluster, so this wont change anything
        closest_centroid = 0
        # calculate eucleadian distance from each centroid for each post point
        for _id, norm in centroid_norms.items():
            dist = math.sqrt((norm[0] - doc_norm[0]) ** 2 + (norm[1] - doc_norm[1]) ** 2)
            # update the min distance and closest centroid to each post
            if dist < min_distance:
                min_distance = dist
                closest_centroid = _id

        # add the squared min distance to get sum of squaured error
        sse += min_distance ** 2
        # update the post cluster with closest centroid id
        doc["cluster"] = closest_centroid
    return tag_posts, sse

def update_centroids(tag_posts, centroids):
    """
    This method updates the centroid values after an iteration of cluster updates.
    
    :params tag_posts: the posts to find distances with centroids. 
    :params centroids: the centroids to find distances between those and posts. 
    :returns updated centroids by calculating the average distance between the points to re-calcuate each centroid. 
    """
    # get centroid norms with ids for mapping
    centroid_norms = {c["_id"]: c["kmeansNorm"] for c in centroids}
    # create a list to hold all update operations
    updated_centroids = []
    
    # iterate over centroids
    for _id, norm in centroid_norms.items():
        # find posts with each cluster for the tag
        cluster_docs = [doc for doc in tag_posts if doc.get("cluster") == _id]
        # get sum norm as length of centroids
        sum_norms = [0] * len(norm)
        # calculate the new centroid; first get sum of kmeansNorms over all cluster documents
        for doc in cluster_docs:
            for i in range(len(doc["kmeansNorm"])):
                sum_norms[i] += doc["kmeansNorm"][i]
        num_docs_in_cluster = len(cluster_docs)
        # just to check if there are documents present
        if num_docs_in_cluster > 0:
            # find the average over the norms for each point
            new_centroid = [x / num_docs_in_cluster for x in sum_norms]
        else:
            # if no documents are in a centroid, keep the old norm
            new_centroid = norm

        # prepare the centroid collection for bulk update
        updated_centroids.append({
            "_id": _id,
            "kmeansNorm": new_centroid
        })
    return updated_centroids


def kmeans_step_execution(sample_size, tag_name):
    """
    This method executes a kmeans step and updates the centroids and clusters.

    :params sample_size: the number of samples (count) for centroids. 
    :params tag_name: the name of the tag to take samples from kmposts. 
    """
    # first re-create the centroids like question 2. this creates initial centroids
    print('first: recreating the centroids with K, T')
    print('K = sample size, T = tag; set in globals.py')
    create_centroids(sample_size, tag_name)
    print(f'centroids collection created for {tag_name} with {sample_size} clusters.')
    print('++++++++++++++++++++++++++++++++++++++++++++++')
    # above code can be moved to q3. but for simiplicity it is moved here.
    # connect to the database
    db = connect()
    # take the posts collection to read data from
    kmposts_collection = db["kmposts"]
    # take the centroids collection to read and write data
    centroid_collection = db["centroids"]
    # find posts with filter by T to avoid mixing with other tags
    posts_clusters_query = [
        {
            "$match": {
                "Tags": {"$in": [tag_name]}
            }
        }
    ]
    tag_posts = list(kmposts_collection.aggregate(posts_clusters_query))
    # get the centroids data (all centroids)
    centroids = list(centroid_collection.find())
    # update cluster for the tag T from globals.py with existing centroids
    tag_posts, see = update_tag_cluster(tag_posts, centroids)
    # recalculate centroids based on updated cluster distances
    centroids = update_centroids(tag_posts, centroids)
    # execute the bulk update for cluster re-assignment
    post_updates = [
            UpdateOne({"_id": doc["_id"]}, {"$set": {"cluster": doc["cluster"]}})
            for doc in tag_posts
        ]
    # execute the bulk update for centroid re-assignment
    centroid_updates = [
            UpdateOne({"_id": centroid["_id"]}, {"$set": {"kmeansNorm": centroid["kmeansNorm"]}})
            for centroid in centroids
        ]
    try:
        kmposts_collection.bulk_write(post_updates)
        centroid_collection.bulk_write(centroid_updates)
    except Exception as e:
        print(f"Error in bulk update: {e}")

def run_kmeans_iterations(all_tags):
    """
    This method executes a kmeans steps for all tags and plots the SSE vs K values.

    :params all_tags: the array of tags specified to run kmeans iterations on. 
    """
    # starting k = 10 up to k = 50 with a step of 5. up to 100 iterations max, conv_threshold is met
    min_k = 10
    step = 5
    max_k = 51
    max_iterations = 100
    # set the convergence level
    conv_threshold = 1e-6
    print(f'Convergence Set to {conv_threshold}')
    # run for all tags
    for tag in all_tags:
        print(f'Processing Tag {tag}')
        # connect to the database
        db = connect()
        # take the posts collection to read data from
        kmposts_collection = db["kmposts"]
        # take the centroids collection to read data and update centroids
        centroid_collection = db["centroids"]
        # list to keep track of see values for plotting for each tag
        sse_values = []
        # run for each K from min to max with step size
        for k in range(min_k, max_k, step):
            # first we create fresh centroids for each k with Tag T. flushes everything already existing.
            create_centroids(k, tag)
            # get the centroids data (all centroids)
            centroids = list(centroid_collection.find())
            # find posts with filter by T to avoid mixing with other tags
            posts_clusters_query = [
                {
                    "$match": {
                        "Tags": {"$in": [tag]}
                    }
                }
            ]
            tag_posts = list(kmposts_collection.aggregate(posts_clusters_query))
            # set the SSE to max to check convergence
            sse_previous = float("inf")
            # run upto max iterations = 100 if does not converge before
            for iteration in range(max_iterations):
                # run an iteration and update cluster for the tag
                tag_posts, sse = update_tag_cluster(tag_posts, centroids)
                # recalculate centroids based on updated cluster distances
                centroids = update_centroids(tag_posts, centroids)
                # get the convergence value (previous - current) error                
                conv_val = abs(sse_previous - sse)
                # print to keep track on consolve; just for user understanding 
                print(f'Iteration {iteration + 1} of {max_iterations}, sse: {sse}, conv_val: {conv_val} ', end="\r")
                # check if convergence (SSE) has stabilized, then break, else continue upto max iterations
                if conv_val < conv_threshold:
                    break
                # update current to previous for next round
                sse_previous = sse
            # once iterations max out or solution converges, then add it to sse_values for plotting
            sse_values.append(sse)
            print(f"k={k}, SSE={sse}")

        # execute the bulk update for cluster re-assignment
        post_updates = [
                UpdateOne({"_id": doc["_id"]}, {"$set": {"cluster": doc["cluster"]}})
                for doc in tag_posts
            ]
        # execute the bulk update for centroid re-assignment
        centroid_updates = [
                UpdateOne({"_id": centroid["_id"]}, {"$set": {"kmeansNorm": centroid["kmeansNorm"]}})
                for centroid in centroids
            ]
        try:
            kmposts_collection.bulk_write(post_updates)
            centroid_collection.bulk_write(centroid_updates)
        except Exception as e:
            print(f"Error in bulk update: {e}")

        # plot the sum of squared errors (SSE) vs k; get k as specified above 
        ks = list(range(min_k, max_k, step))
        # first clear the plot to avoid re-writing to different tag plots
        plt.clf()
        # plot ks vs sse values
        plt.plot(ks, sse_values, marker='o')
        plt.xlabel('Clusters (K)')
        plt.ylabel('Sum of Squared Errors (SSE)')
        plt.title(f'SSE vs K - Tag: {tag}')
        # save the figure in the root 
        plt.savefig(f'sse_plot_{tag}')
        # just notify to user
        print(f'plot: sse_plot_{tag} saved in the root folder')

def create_cluster_posts_collection(kmposts_collection, centroid_collection, tag):
    """
    This function creates a new collection with posts that belong to a particular centroid's cluster.
    
    :param kmposts_collection: the posts collection
    :param centroid_collection: the centroids collection
    :param tag: the tag for filtering posts in the kmposts collection
    """
    # get the centroids data (all centroids)
    centroids = list(centroid_collection.find())
    if not centroids:
        print(f"No centroids found. Rerun centroid creation for tag '{tag}'")
        return
    # select any random centroid/cluster to present example
    import random
    random_centroid = random.choice(centroids)
    cluster_id = random_centroid["_id"]
    # just to verify; print out
    print(f"Selected cluster: {cluster_id} for tag {tag} from centroid {random_centroid} to take subset of posts")
    # find posts with each cluster from centroid ids also filter by T to avoid mixing of clusters with from other tags
    posts_clusters_query = [
        {
            "$match": {
                "Tags": {"$in": [tag]},
                "cluster": cluster_id
            }
        }
    ]
    posts_in_cluster = list(kmposts_collection.aggregate(posts_clusters_query))
    # just to verify; print out
    print(f"Found {len(posts_in_cluster)} posts in cluster {cluster_id} for tag '{tag}'.")
    if not posts_in_cluster:
        print(f"No posts found for tag '{tag}' in cluster {cluster_id}.")
        return    
    # otherwise create a subset collection based on tag and cluster_id
    collection_name = f"posts_{tag}_cluster{cluster_id}"
    # re=connect to the database
    db = connect()
    new_collection = db[collection_name]
    # create cluster posts collection for data explanation
    try:
        new_collection.insert_many(posts_in_cluster)
        print(f"Created collection {collection_name} with {len(posts_in_cluster)} posts.")
    except Exception as e:
        print(f"Error while inserting posts into new collection: {e}")

def run_kmeans_best_cluster(tag, best_K):
    """
    This method executes a kmeans steps for each tag on best value of K to produce sample outputs for analysis.

    :params tag: the tag to run kmeans and create subset of output. 
    :params best_K: the value of K to run kmeans. 
    """
    # up to 100 iterations max, conv_threshold is met
    max_iterations = 100
    # set the convergence level
    conv_threshold = 1e-6
    print(f'Convergence Set to {conv_threshold}')
    # connect to the database
    db = connect()
    # take the posts collection to read data from
    kmposts_collection = db["kmposts"]
    # take the centroids collection to read data from
    centroid_collection = db["centroids"]
    # first we create fresh centroids for best k with Tag T.
    create_centroids(best_K, tag)
    # get the centroids data (all centroids)
    centroids = list(centroid_collection.find())
    # find posts with filter by T to avoid mixing with other tags
    posts_clusters_query = [
        {
            "$match": {
                "Tags": {"$in": [tag]}
            }
        }
    ]
    tag_posts = list(kmposts_collection.aggregate(posts_clusters_query))
    # set the SSE to max to check convergence
    sse_previous = float("inf")
    # run upto max iterations = 100 if does not converge before
    for iteration in range(max_iterations):
        # run an iteration and update cluster for the tag
        tag_posts, sse = update_tag_cluster(tag_posts, centroids)
        # recalculate centroids based on updated cluster distances
        centroids = update_centroids(tag_posts, centroids)
        # get the convergence value (previous - current) error 
        conv_val = abs(sse_previous - sse)
        # print to keep track on consolve; just for user understanding 
        print(f'Iteration {iteration + 1} of {max_iterations}, sse: {sse}, conv_val: {conv_val} ', end="\r")
        # check if convergence (SSE) has stabilized, then break, else continue upto max iterations
        if conv_val < conv_threshold:
            break
        # update current to previous for next round
        sse_previous = sse
    
    # execute the bulk update for cluster re-assignment
    post_updates = [
            UpdateOne({"_id": doc["_id"]}, {"$set": {"cluster": doc["cluster"]}})
            for doc in tag_posts
        ]
    # execute the bulk update for centroid re-assignment
    centroid_updates = [
            UpdateOne({"_id": centroid["_id"]}, {"$set": {"kmeansNorm": centroid["kmeansNorm"]}})
            for centroid in centroids
        ]
    try:
        kmposts_collection.bulk_write(post_updates)
        centroid_collection.bulk_write(centroid_updates)
    except Exception as e:
        print(f"Error in bulk update: {e}")
    
    # once iterations max out or solution converges, then create a sub-collection for tag with a cluster for analysis
    create_cluster_posts_collection(kmposts_collection, centroid_collection, tag)

    
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
        print(f"Collection: {collection_name}   Docs: {count}")
        # If the collection is kmposts, or posts count the comments
        if collection_name == "posts":
            total_tags = 0
            # Iterate through each document in the collection
            for document in db[collection_name].find():
                tags = document.get('Tags', [])
                total_tags += len(tags)
            print(f"Collection: {collection_name}   Tags: {total_tags}")
        if collection_name == "kmposts":
            total_tags = 0
            # Iterate through each document in the collection
            for document in db[collection_name].find():
                tags = document.get('Tags', [])
                total_tags += len(tags)
            print(f"Collection: {collection_name}   Tags: {total_tags}")


from funcs.globals import connect

def q2_query1(explain=False):
    """
    Q. 2.1: Names of the top 10 most popular badges earned by users within a year of creating their accounts.

    :param explain: controls what to return (exectuion plan (True)); by default False.
    :returns query results by default (explain = True) or execution plan (explain = True).
    """
    # connect to database
    db = connect()
    # users collection
    users_collection = db['users']
    # create the query
    query = [
        # first unwind all badges documents
        { "$unwind": "$Badges" },
        # then match Date is within one year (timestamp format)
        {
            "$match": {
                "$expr": {
                    "$lt": ["$Badges.Date", { "$add": ["$CreationDate", 365 * 24 * 60 * 60 * 1000] }]
                }
            }
        },
        # group (sum) by Name
        {
            "$group": {
                "_id": "$Badges.Name",
                "count": { "$sum": 1 }
            }
        },
        # sort descending and limt to 10 records
        { "$sort": { "count": -1 } },
        { "$limit": 10 },
        # project the names and count
        {
            "$project": {
                "_id": 0,
                "badgeName": "$_id",
                "count": 1
            }
        }
    ]
    # if explain is true, return execution plan, otherwise return query resutls
    if explain:
        return db.command('aggregate', 'users', pipeline=query, explain=True)
    return users_collection.aggregate(query)

def q2_query2(explain=False):
    """
    Q. 2.2: Display names of users who have never posted but have a reputation greater than 1,000.

    :param explain: controls what to return (exectuion plan (True)); by default False.
    :returns query results by default (explain = True) or execution plan (explain = True).
    """
    # connect to database
    db = connect()
    # start with users collection
    users_collection = db['users'] 
    # create the query
    query = [
        # filter users with Reputation > 1000; but Reputation is not an attribute in the collection as suggested by assignment task, so it will be empty. 
        { "$match": { "Reputation": { "$gt": 1000 } } },
        # then lookup with posts collection joining on OwnerUserId (posts) and _id (users) 
        {
            "$lookup": {
                "from": "posts",
                "localField": "_id",
                "foreignField": "OwnerUserId",
                "as": "userPosts"
            }
        },
        # filter users who have never posted 
        { "$match": { "userPosts": { "$eq": [] } } },
        # finally return users display name
        {
            "$project": {
                "_id": 0,
                "DisplayName": 1
            }
        }
    ]
    # if explain is true, return execution plan, otherwise return query resutls
    if explain:
        return db.command('aggregate', 'users', pipeline=query, explain=True)
    return users_collection.aggregate(query)

def q2_query3(explain=False):
    """
    Q. 2.3: Display name and reputation of users who have answered more than one question with the tag postgresql.

    :param explain: controls what to return (exectuion plan (True)); by default False.
    :returns query results by default (explain = True) or execution plan (explain = True).
    """
    # connect to database
    db = connect()
    # start with posts collection
    posts_collection = db['posts'] 
    # create the query
    query = [
        # from AskUbuntu I assume (PostTypeId: 2) is answer; 
        # filter on postTypeId = 2 and tag postgresql
        {
            "$match": {
                "PostTypeId": 2,
                "Tags": { "$in": ["postgresql"] }
            }
        },
        # Group (sum) posts by OwnerUserId
        {
            "$group": {
                "_id": "$OwnerUserId",
                "answerCount": { "$sum": 1 }
            }
        },
        # Match users answers: more than one question
        { "$match": { "answerCount": { "$gt": 1 } } },
        # then lookup with users collection joining on OwnerUserId (posts) as mapped above "_id": "$OwnerUserId", and _id (users)
        {
            "$lookup": {
                "from": "users",
                "localField": "_id",
                "foreignField": "_id",
                "as": "userDetails"
            }
        },
        # unwind all matched user joined documents
        { "$unwind": "$userDetails" },
        # Project the desired fields, as the collections do not contain the reputation hence skipping it in projection
        {
            "$project": {
                "_id": 0,
                "DisplayName": "$userDetails.DisplayName"
                # "Reputation": "$userDetails.Reputation"
            }
        }
    ]
    # if explain is true, return execution plan, otherwise return query resutls
    if explain:
        return db.command('aggregate', 'posts', pipeline=query, explain=True)
    return posts_collection.aggregate(query)

def q2_query4(explain=False):
    """
    Q. 2.4: Display name of users who posted comments with a score greater than 10 within the first week of creating their accounts.

    :param explain: controls what to return (exectuion plan (True)); by default False.
    :returns query results by default (explain = True) or execution plan (explain = True).
    """
    # connect to database
    db = connect()
    # start with posts collection
    posts_collection = db['posts'] 
    # create the query
    query = [
        # first filter posts with comment score > 10
        {
            "$match": {
                "Comments.Score": { "$gt": 10 }
            }
        },
        # Unwind the Comments array
        { "$unwind": "$Comments" },
        # Lookup with users collection joining on Comments.UserId (posts), and _id (users)
        {
            "$lookup": {
                "from": "users",
                "localField": "Comments.UserId",
                "foreignField": "_id",
                "as": "userDetails"
            }
        },
        # Unwind userDetails 
        { "$unwind": "$userDetails" },
        # check is if comments creation date is within 7 days of user account creation.
        {
            "$match": {
                "$expr": {
                    "$lt": [
                        "$Comments.CreationDate", 
                        { "$add": ["$userDetails.CreationDate", 7 * 24 * 60 * 60 * 1000] }
                    ]
                }
            }
        },
        # Project those user names
        {
            "$project": {
                "_id": 0,
                "DisplayName": "$userDetails.DisplayName"
            }
        }
    ]
    # if explain is true, return execution plan, otherwise return query resutls
    if explain:
        return db.command('aggregate', 'posts', pipeline=query, explain=True)
    return posts_collection.aggregate(query)

def q2_query5(explain=False):
    """
    Q. 2.5: The tag names of the tags most commonly used on posts along with the tag postgresql and the count of each tag.

    :param explain: controls what to return (exectuion plan (True)); by default False.
    :returns query results by default (explain = True) or execution plan (explain = True).
    """
    # connect to database
    db = connect()
    # start with posts collection
    posts_collection = db['posts'] 
    # create the query
    query = [
        # first tag postgresql
        {
            "$match": {
                "Tags": { "$in": ["postgresql"] }
            }
        },
        # Unwind the Tags array
        { "$unwind": "$Tags" },
        # Group (sum) by Tags
        {
            "$group": {
                "_id": "$Tags",
                "count": { "$sum": 1 } 
            }
        },
        # Sort by count in descending
        {
            "$sort": {
                "count": -1  #  order
            }
        }
    ]
    # if explain is true, return execution plan, otherwise return query resutls
    if explain:
        return db.command('aggregate', 'posts', pipeline=query, explain=True)
    return posts_collection.aggregate(query)

def q4_index_creation():
    """
    create indexes to optimize the query results.
    """
    # connect to the database.
    db = connect()
    # get the users collection
    users_collection = db['users'] 
    # create index on users to optimize comparing, creation dates and badges dates. Other indexes do not add value
    users_collection.create_index([("CreationDate", 1)], name='index_user_creation_date')
    users_collection.create_index([("Badges.Date", 1)], name='index_badges_date')
    
    # re-connect to the database to avoid cursor cache issue
    db = connect()
    # get the posts collection
    posts_collection = db['posts'] 
    # create index on posts to optimize comparing creation dates, tags, comments creation dates, comments score, and reference keys to users, Other indexes do not add value
    posts_collection.create_index([("OwnerUserId", 1)], name='index_post_owner_id')
    posts_collection.create_index([("Tags", 1)], name='index_post_tags')
    posts_collection.create_index([("CreationDate", 1)], name='index_post_creation_date')
    
    posts_collection.create_index([("Comments.UserId", 1)], name='index_comments_user_id')
    posts_collection.create_index([("Comments.Score", 1)], name='index_comments_score')
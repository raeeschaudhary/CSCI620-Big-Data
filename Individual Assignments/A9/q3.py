import time
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, split, array_contains, count
from globals import data_directory

if __name__=="__main__":
    start_time = time.time()

    # QUERY 3
    print('Running Query 3.')
    print('++++++++++++++++++++++++++++++++++++++++++++++')
    print('The display names of users with the most number of comments on posts with the tag “networking”.')
    print('++++++++++++++++++++++++++++++++++++++++++++++')

    # create spark session
    spark = SparkSession.builder.getOrCreate()
    # define the source datasets and rename the primary columns for clarity
    users = spark.read.json(data_directory + "Users.json").withColumnRenamed("id", "user_id")
    posts = spark.read.json(data_directory + "Posts.json").withColumnRenamed("id", "post_id")
    comments = spark.read.json(data_directory + "Comments.json").withColumnRenamed("id", "comment_id")

    # first convert tags to an array, remove the outer angle brackets, exploding is needed, as only posts needed with one tag.
    posts_with_tags = posts.withColumn("tags_array", split(col("tags"), "<|>"))
    # filter tags array to contain only networking posts
    posts_networking = posts_with_tags.filter(array_contains(col("tags_array"), "networking"))

    # join with comments to get comments for networking posts; inner joins only takes comments matching with networking posts
    comments_networking = posts_networking.join(comments, posts_networking.post_id == comments.postid, "inner")
    # group networking comment counts by user id to get comments for each user
    comments_networking_users = comments_networking.groupBy("userid").agg(count("comment_id").alias("comment_count"))
    # join user commetns with users to get display names, inner joins only takes users matching with networking comment users
    user_networking_comments = comments_networking_users.join(users, comments_networking_users.userid == users.user_id, "inner")

    # sort the number of comments in descending order
    results = user_networking_comments.select(col("displayname"), col("comment_count")).orderBy(col("comment_count").desc())

    # report the result count
    print('++++++++++++++++++++++++++++++++++++++++++++++')
    print(f'Records Returned: {results.count()}')
    print('++++++++++++++++++++++++++++++++++++++++++++++')
    # display the first 10 rows
    print(results.show(10))
    print('++++++++++++++++++++++++++++++++++++++++++++++')
    
    end_time = time.time()
    run_time = end_time - start_time
    print("Total running time: ", run_time, " seconds")
    print('++++++++++++++++++++++++++++++++++++++++++++++')
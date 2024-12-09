import time
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, year, to_timestamp
from globals import data_directory

if __name__=="__main__":
    start_time = time.time()

    # QUERY 1
    print('Running Query 1.')
    print('++++++++++++++++++++++++++++++++++++++++++++++')
    print('Users whose display name starts with J and did not make any posts in 2014.')
    print('++++++++++++++++++++++++++++++++++++++++++++++')

    # create spark session
    spark = SparkSession.builder.getOrCreate()
    # define the source datasets and rename the primary columns for clarity
    users = spark.read.json(data_directory + "Users.json").withColumnRenamed("id", "user_id")
    posts = spark.read.json(data_directory + "Posts.json").withColumnRenamed("id", "post_id")
    
    # filter users whose display name starts with J or j (upper and lower both included)
    j_users = users.filter(col("displayname").rlike("(?i)^j"))

    # for date (year) comparison, convert the post creation date to timestamp (using the given format) - rename also avoid ambiguity with users creation date
    date_formatted_posts = posts.withColumn("postcreationdate", to_timestamp("creationdate", "yyyy-MM-dd'T'HH:mm:ss.SSS"))
    # filter the posts to contain for the year 2014, to exclude users
    posts_of_2014 = date_formatted_posts.filter(year(col("postcreationdate")) == 2014)
    
    # get user IDs who made posts in 2014, to exclude those from users
    users_of_2014_posts = posts_of_2014.select("owneruserid").distinct()
    # perform a left anti join to find (J/j) users who did NOT make posts in 2014. taking all users that are not in users_of_2014_posts
    users_wo_posts_2014 = j_users.join(users_of_2014_posts, j_users.user_id == users_of_2014_posts.owneruserid, "leftanti")
    
    # only select needed columns (id, display name) for displaying, sorted ascendingly by display name
    results = users_wo_posts_2014.select("user_id", "displayname").orderBy(col("user_id").desc())

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
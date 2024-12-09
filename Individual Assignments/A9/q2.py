import time
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, year, to_timestamp, split, explode, count, trim
from globals import data_directory

if __name__=="__main__":
    start_time = time.time()

    # QUERY 2
    print('Running Query 2.')
    print('++++++++++++++++++++++++++++++++++++++++++++++')
    print('The most popular tags on posts created in 2017.')
    print('++++++++++++++++++++++++++++++++++++++++++++++')
    
    # create spark session
    spark = SparkSession.builder.getOrCreate()
    # define the source datasets and rename the primary columns for clarity
    posts = spark.read.json(data_directory + "Posts.json").withColumnRenamed("id", "post_id")

    # for date (year) comparison, convert the post creation date to timestamp (using the given format) - rename also avoid ambiguity with users creation date
    date_formatted_posts = posts.withColumn("postcreationdate", to_timestamp("creationdate", "yyyy-MM-dd'T'HH:mm:ss.SSS"))
    # filter the posts to contain for the year 2017
    posts_of_2017 = date_formatted_posts.filter(year(col("postcreationdate")) == 2017)

    # convert tags to an array, remove the outer angle brackets and explode tags into rows
    posts_with_tags = posts_of_2017.withColumn("tags_array", split(col("tags"), "<|>")).withColumn("tag", explode(col("tags_array")))
    # trim whitespaces and filter out empty strings; this is important as spliting may results in empty space considered as tag
    posts_with_tags_clean = posts_with_tags.filter(trim(col("tag")) != "")

    # count occurance of each tag and group by tag and then sort desc by count, creating an alias
    results = posts_with_tags_clean.groupBy("tag").agg(count("tag").alias("tag_count")).orderBy(col("tag_count").desc())

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
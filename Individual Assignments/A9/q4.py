import time
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count
from globals import data_directory

if __name__=="__main__":
    start_time = time.time()

    # QUERY 4
    print('Running Query 4.')
    print('++++++++++++++++++++++++++++++++++++++++++++++')
    print('The most popular badges earned by users with less than 100 reputation.')
    print('++++++++++++++++++++++++++++++++++++++++++++++')

    # create spark session
    spark = SparkSession.builder.getOrCreate()
    # define the source datasets and rename the primary columns for clarity
    users = spark.read.json(data_directory + "Users.json").withColumnRenamed("id", "user_id")
    badges = spark.read.json(data_directory + "Badges.json").withColumnRenamed("id", "badge_id")

    # first filter users with reputation less than 100
    users_rep100 = users.filter(col("reputation") < 100)
    # then join users with badges based on user id
    badges_rep100_users = users_rep100.join(badges, users_rep100.user_id == badges.userid, "inner")
    # count occurance of each badge and group by name and then sort by count
    results = badges_rep100_users.groupBy("name").agg(count("name").alias("badge_count")).orderBy(col("badge_count").desc())
    
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
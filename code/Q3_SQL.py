from pyspark.sql import SparkSession
import time
import sys
import json
spark = SparkSession.builder.appName("Q3-SQL").getOrCreate()

file_format = sys.argv[1]

if file_format == 'parquet' :
    df_genres = spark.read.parquet("hdfs://master:9000/files/movie_genres.parquet")

    df_ratings = spark.read.parquet("hdfs://master:9000/files/ratings.parquet")
elif file_format == 'csv' :
    df_genres = spark.read.format('csv').options(header='false', sep=",", inferSchema='true').load("hdfs://master:9000/files/movie_genres.csv")

    df_ratings = spark.read.format('csv').options(header='false', sep="," , inferSchema='true').load("hdfs://master:9000/files/ratings.csv")

df_genres.registerTempTable("df_genres")
df_ratings.registerTempTable("df_ratings")

query = \
    "select g._c1 as genre, avg(averagebymovies) as average_rating, count(*) as number " + \
    "from (select " + \
                "r._c1 as movie_id, AVG(r._c2) as averagebymovies " + \
                "from df_ratings as r " + \
                "group by r._c1) " + \
    "join df_genres as g on g._c0 = movie_id " + \
    "group by g._c1 " +\
    "order by g._c1"

t1 = time.time()
res = spark.sql(query)

if file_format == 'parquet':
    nam = "q3-sql-parquet"
    namef = "hdfs://master:9000/outputs/"+nam+".csv"
elif file_format== "csv":
    nam = "q3-sql-csv"
    namef = "hdfs://master:9000/outputs/"+nam+".csv"

res.coalesce(1).write.format('csv').save(namef)
t2 = time.time()
a_file = open("times.json", "r")
a_dictionary = json.load(a_file)
a_dictionary.update({nam : t2-t1})

a_file = open("times.json", "w")
a_file = json.dump(a_dictionary, a_file)

from pyspark.sql import SparkSession
import time
import sys
import json
spark = SparkSession.builder.appName("query5-rdd").getOrCreate()


file_format = sys.argv[1]
t1 = time.time()
if file_format == 'parquet' :
    movies = spark.read.parquet("hdfs://master:9000/files/movies.parquet")
    categories  = spark.read.parquet("hdfs://master:9000/files/movie_genres.parquet")
    ratings = spark.read.parquet("hdfs://master:9000/files/ratings.parquet")
elif file_format == 'csv' :
    movies = spark.read.format("csv").options(header = 'false',inferSchema='true').load("hdfs://master:9000/files/movies.csv")
    categories = spark.read.format("csv").options(header ='false',inferSchema='true').load("hdfs://master:9000/files/movie_genres.csv")
    ratings = spark.read.format("csv").options(header = 'false', inferSchema='true').load("hdfs://master:9000/files/ratings.csv")
movies.registerTempTable("movies")
categories.registerTempTable("categories")
ratings.registerTempTable("ratings")


sqlString = "WITH shop AS (SELECT c._c1,r._c0,count(*) as c1 FROM movies as m, categories as c, ratings as r where r._c1 == m._c0 and m._c0 == c._c0 group by c._c1,r._c0 order by c._c1 ,c1 desc)" + \
"SELECT _c1 AS cat,_c0 AS user,c1 AS rating FROM shop s1 WHERE c1 = (SELECT MAX(s2.c1)  FROM shop s2 WHERE s1._c1 = s2._c1)"
res = spark.sql(sqlString)
res.registerTempTable("user_cat")

sqlComString =  "SELECT m._c0 AS movie_id ,m._c1 AS movie_title, m._c7 AS movie_pop,c._c1 AS movie_genre FROM movies as m, categories as c WHERE m._c0 = c._c0"
res_mg = spark.sql(sqlComString)
res_mg.registerTempTable("movie_genre")

sqlOtherString ="SELECT u.cat AS Category,u.user AS User,u.rating AS Votes, r._c2 AS rating ,mg.movie_pop AS popularity,mg.movie_title AS movieid FROM user_cat AS u, ratings as r,movie_genre as mg where u.user = r._c0 and u.cat = mg.movie_genre and r._c1 = mg.movie_id"
resfg = spark.sql(sqlOtherString)
resfg.registerTempTable("user_rat")

sqlFinString = "SELECT o.* FROM user_rat o LEFT JOIN user_rat b ON (o.Category = b.Category AND o.User = b.User AND o.rating < b.rating) WHERE b.rating is NULL"
resff = spark.sql(sqlFinString)
resff.registerTempTable("user_best_rat")

sqlAFin = "SELECT o.* FROM user_best_rat o LEFT JOIN user_best_rat b ON (o.Category = b.Category AND o.User = b.User AND o.popularity < b.popularity) WHERE b.popularity is NULL"
afin = spark.sql(sqlAFin)
afin.registerTempTable("user_best")

sqlWorst = "SELECT o.* FROM user_rat o LEFT JOIN user_rat b ON (o.Category = b.Category AND o.User = b.User AND o.rating >  b.rating) WHERE b.rating is NULL"
sw = spark.sql(sqlWorst)
sw.registerTempTable("sw")


sqlo = "SELECT o.* FROM sw o LEFT JOIN sw b ON (o.Category = b.Category AND o.User = b.User AND o.popularity < b.popularity) WHERE b.popularity is NULL"
sq = spark.sql(sqlo)
sq.registerTempTable("sq")

sql_merge = "SELECT ub.Category,ub.User,ub.movieid AS best_movie,ub.Votes,ub.rating AS best_rating,user_worst.movieid AS worst_movie,user_worst.rating AS worst_rating FROM user_best ub JOIN sq user_worst ON (ub.Category = user_worst.Category and ub.User = user_worst.User) ORDER BY ub.Category ASC"

res = spark.sql(sql_merge)
if file_format == 'parquet':
    nam = "q5-sql-parquet"
    namef = "hdfs://master:9000/outputs/"+nam+".csv"
elif file_format== "csv":
    nam = "q5-sql-csv"
    namef = "hdfs://master:9000/outputs/"+nam+".csv"

res.coalesce(1).write.format('csv').save(namef)
t2 = time.time()

print("\nExecution time:")
print("--- %s seconds ---" % '{:.20f}'.format(t2-t1))

a_file = open("times.json", "r")
a_dictionary = json.load(a_file)
a_dictionary.update({nam : t2-t1})

a_file = open("times.json", "w")
a_file = json.dump(a_dictionary, a_file)

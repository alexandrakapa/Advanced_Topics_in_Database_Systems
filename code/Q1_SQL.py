from pyspark.sql import SparkSession
import time
import sys
spark = SparkSession.builder.appName("Q1-SQL").getOrCreate()
import json
file_format = sys.argv[1]

if file_format == 'parquet' :
    df_movies = spark.read.parquet("hdfs://master:9000/files/movies.parquet")
elif file_format == 'csv' :
    df_movies = spark.read.format("csv").options(header = 'false',inferSchema='true').load("hdfs://master:9000/files/movies.csv")

df_movies.registerTempTable("df_movies")

sqlString = \
    "select *, ((_c6-_c5)/_c5)*100 as kerdos " + \
    "from df_movies " + \
    "where (year(_c3), ((_c6-_c5)/_c5)*100) in (select year(_c3), max(((_c6-_c5)/_c5)*100) as kerdos from df_movies where year(_c3) >= 2000 group by year(_c3))"

t1 = time.time()
res = spark.sql(sqlString)

res.show()
if file_format == 'parquet':
    nam = "q1-sql-parquet"
    namef = "hdfs://master:9000/outputs/"+nam+".csv"
elif file_format== "csv":
    nam = "q1-sql-csv"
    namef = "hdfs://master:9000/outputs/"+nam+".csv"

res.coalesce(1).write.format('csv').save(namef)
t2 = time.time()


a_file = open("times.json", "r")
a_dictionary = json.load(a_file)
a_dictionary.update({nam : t2-t1})

a_file = open("times.json", "w")
a_file = json.dump(a_dictionary, a_file)

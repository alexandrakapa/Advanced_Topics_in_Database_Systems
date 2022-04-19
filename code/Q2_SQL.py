from pyspark.sql import SparkSession
from pyspark.sql.functions import col, countDistinct
import sys
import time
import json
from pyspark.sql.types import FloatType
spark = SparkSession.builder.appName("Q2-SQL").getOrCreate()

file_format = sys.argv[1]

if file_format == 'parquet' :
    df_ratings = spark.read.parquet("hdfs://master:9000/files/ratings.parquet")
elif file_format == 'csv' :
    df_ratings = spark.read.format("csv").options(header = 'false',inferSchema='true').load("hdfs://master:9000/files/ratings.csv")

df_ratings.registerTempTable("df_ratings")


all_users = "select " + \
                "DISTINCT r._c0 " + \
            "from df_ratings as r"
t1 = time.time()
all_users = spark.sql(all_users)
all_users = all_users.count()


some_users = "select * " + \
            "from (select DISTINCT r._c0 from df_ratings as r group by r._c0 having AVG(r._c2) > 3.0)"

some_users = spark.sql(some_users)
some_users = some_users.count()

if file_format == 'parquet':
    nam = "q2-sql-parquet"
    namef = "hdfs://master:9000/outputs/"+nam+".csv"
elif file_format== "csv":
    nam = "q2-sql-csv"
    namef = "hdfs://master:9000/outputs/"+nam+".csv"


percentage = (some_users/all_users)*100
print(percentage)

list = []
list.append(percentage)

rdd_perc = spark.sparkContext.parallelize(list)
dataframe_perc = spark.createDataFrame(rdd_perc,FloatType())

dataframe_perc.write.format('csv').save(namef)
t2 = time.time()

a_file = open("times.json", "r")
a_dictionary = json.load(a_file)
a_dictionary.update({nam : t2-t1})

a_file = open("times.json", "w")
a_file = json.dump(a_dictionary, a_file)

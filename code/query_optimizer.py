from pyspark.sql import SparkSession
import sys, time
import json

disabled = sys.argv[1]

spark = SparkSession.builder.appName('Query_Optimizer').getOrCreate()

# According to the documentation the option autoBroadcastJoinThreshold
# configures the maximum size in bytes for a table that will be broadcast
# to all worker nodes when performing a join. By setting this value to -1
# broadcasting can be disabled.
if disabled == "Y":
    spark.conf.set("spark.sql.autoBroadcastJoinThreshold", -1)
elif disabled == 'N':
    pass
else:
    raise Exception ("This setting is not available.")

df = spark.read.format("parquet")

df1 = df.load("hdfs://master:9000/files/ratings.parquet")
df2 = df.load("hdfs://master:9000/files/movie_genres.parquet")

df1.registerTempTable("ratings")
df2.registerTempTable("movie_genres")

sqlString = \
"SELECT * " + \
"FROM " + \
"       (SELECT * FROM movie_genres LIMIT 100) as g, " + \
"       ratings as r " + \
"WHERE " + \
"       r._c1 = g._c0"

t1 = time.time()
res = spark.sql(sqlString)
res.show(res.count(), False)
t2 = time.time()

spark.sql(sqlString).explain()
print("Time with choosing join type %s is %.4f sec."%("enabled" if disabled == 'N' else "disabled", t2-t1))

a_file = open("times.json", "r")
a_dictionary = json.load(a_file)
a_dictionary.update({"optimizer-Y" : t2-t1})

a_file = open("times.json", "w")
a_file = json.dump(a_dictionary, a_file)

from pyspark.sql import SparkSession
from statistics import mean
import time
import json
spark = SparkSession.builder.appName("Q3-RDD").getOrCreate()

sc = spark.sparkContext
t1 = time.time()

genres = sc.textFile("hdfs://master:9000/files/movie_genres.csv"). \
	  map(lambda x : (x.split(",")[1], int(x.split(",")[0]))). \
	  map(lambda x : (x[1], x[0]))


ratings = sc.textFile("hdfs://master:9000/files/ratings.csv"). \
	  map(lambda x : (int(x.split(",")[1]), float(x.split(",")[2]))). \
	  groupByKey(). \
	  map(lambda x : (x[0], mean(sorted(x[1]))))

joined = genres.join(ratings). \
	map(lambda x : (x[1][0], x[1][1]))

further = joined.groupByKey(). \
	map(lambda x: (x[0], mean(sorted(x[1])), len(sorted(x[1])))). \
	sortBy(lambda x : x[0])

for i in further.collect():
	print(i)


def toCSVLine(data):
  return ','.join(str(d) for d in data)
lines = further.map(toCSVLine)

lines.saveAsTextFile('hdfs://master:9000/outputs/Q3_RDD.csv')
t2 = time.time()

a_file = open("times.json", "r")
a_dictionary = json.load(a_file)
a_dictionary.update({"q3-rdd" : t2-t1})

a_file = open("times.json", "w")
a_file = json.dump(a_dictionary, a_file)

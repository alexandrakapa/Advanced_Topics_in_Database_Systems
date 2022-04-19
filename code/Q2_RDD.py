from pyspark.sql import SparkSession
from io import StringIO
import csv
from statistics import mean
import time
import json
from pyspark.sql.types import FloatType
spark = SparkSession.builder.appName("Q2-RDD").getOrCreate()

sc = spark.sparkContext

read = sc.textFile("hdfs://master:9000/files/ratings.csv"). \
	  map(lambda x : (int(x.split(",")[0]), float(x.split(",")[2])))

t1 = time.time()
all_users = read.groupByKey(). \
	  mapValues(set). \
	  mapValues(sorted). \
	  sortByKey(). \
	  count()

some_users = read.groupByKey(). \
	 map(lambda x: (x[0], mean(sorted(x[1])))). \
	 sortByKey(). \
	 filter(lambda x : x[1] > 3.0). \
	 count()


percentage = (some_users/all_users)*100


t2 = time.time()
def toCSVLine(data):
  return ','.join(str(d) for d in data)

list = []
list.append(percentage)

# lines = list.map(toCSVLine)
rdd_perc = spark.sparkContext.parallelize(list)
dataframe_perc = spark.createDataFrame(rdd_perc,FloatType())

dataframe_perc.write.format('csv').save('hdfs://master:9000/outputs/Q2_RDD.csv')

a_file = open("times.json", "r")
a_dictionary = json.load(a_file)
a_dictionary.update({"q2-rdd" : t2-t1})

a_file = open("times.json", "w")
a_file = json.dump(a_dictionary, a_file)

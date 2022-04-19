from pyspark.sql import SparkSession
from io import StringIO
from pyspark.sql.functions import year, to_date
from datetime import datetime
import csv
import pyspark.sql.functions as func
import time
import json
def split_complex(x):
	return list(csv.reader(StringIO(x), delimiter=","))[0]

spark = SparkSession.builder.appName("Q1-RDD").getOrCreate()

sc = spark.sparkContext
t1 = time.time()

res = sc.textFile("hdfs://master:9000/files/movies.csv"). \
	  map(lambda x : (int(split_complex(x)[0]), split_complex(x)[1], split_complex(x)[3], int(split_complex(x)[5]), int(split_complex(x)[6]))). \
	  filter(lambda x : x[3] > 0). \
	  filter(lambda x : x[4] > 0). \
	  filter(lambda x : int((x[2].split("-"))[0]) >= 2000)


res = res.collect()
length = len(res)
# we have the gross in every movie tuple
for i in range(length):
		gross = ((res[i][4] - res[i][3])/res[i][3])*100
		res[i] = list(res[i])
		res[i].append(gross)
		res[i] = tuple(res[i])


unique_values = set(int((tuple[2].split("-"))[0]) for tuple in res)
group_list = [[list for list in res if int((list[2].split("-"))[0]) == value] for value in unique_values]

best_list = []
for i in range(len(group_list)):
	max_value = max(group_list[i],key = lambda item:item[5])
	best_list.append(max_value)


def toCSVLine(data):
  return ','.join(str(d) for d in data)

best_list = sc.parallelize(best_list)
lines = best_list.map(toCSVLine)

# for i in lines.collect():
# 	print(i)
lines.saveAsTextFile('hdfs://master:9000/outputs/Q1_RDD.csv')
t2 = time.time()
print(t2-t1)

a_file = open("times.json", "r")
a_dictionary = json.load(a_file)
a_dictionary.update({"q1-rdd" : t2-t1})

a_file = open("times.json", "w")
a_file = json.dump(a_dictionary, a_file)

from pyspark.sql import SparkSession
from pyspark import RDD
import time
import json
import os.path

spark = SparkSession.builder.appName("Broadcast_join").getOrCreate()

sc = spark.sparkContext

t1 = time.time()

def helper(list):
        bucket1 = []
        bucket2 = []
        for it in list:
                if it[0] == 'ratings':bucket1.append(it)
                elif it[0] == 'genres':bucket2.append(it)
        fin = [(v, w) for v in bucket1 for w in bucket2]
        return fin
def repartition_join(add1,add2):
        file1 = sc.textFile(add1). \
            map(lambda x : (int(x.split(",")[1]), ("ratings",int(x.split(",")[0]), int(x.split(",")[1]), float(x.split(",")[2]), int(x.split(",")[3]))))

        file2 = sc.textFile(add2). \
            map(lambda x : (int(x.split(",")[0]), ("genres",x.split(",")[1])))
        common_file = file1.union(file2)
        final = common_file.groupByKey().flatMapValues(lambda x : helper(x)).map(lambda x : (x[0], x[1][0][1],x[1][0][3],x[1][0][4], x[1][1][1]))
        return final

final = repartition_join("hdfs://master:9000/files/ratings.csv","hdfs://master:9000/files/100_movie_genres.csv")


def toCSVLine(data):
  return ','.join(str(d) for d in data)
lines = final.map(toCSVLine)

lines.saveAsTextFile('hdfs://master:9000/outputs/repartition_join.csv')

t2 = time.time()

a_file = open("times.json", "r")
a_dictionary = json.load(a_file)
a_dictionary.update({"repartition_join" : t2-t1})

a_file = open("times.json", "w")
a_file = json.dump(a_dictionary, a_file)

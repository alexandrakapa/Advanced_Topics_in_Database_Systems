from pyspark.sql import SparkSession
from pyspark import RDD
import time
import json
spark = SparkSession.builder.appName("Broadcast_join").getOrCreate()

sc = spark.sparkContext

first_file = sc.textFile("hdfs://master:9000/files/ratings.csv"). \
            map(lambda x : (int(x.split(",")[1]), (int(x.split(",")[0]), int(x.split(",")[1]), float(x.split(",")[2]), int(x.split(",")[3]))))

second_file = sc.textFile("hdfs://master:9000/files/100_movie_genres.csv"). \
            map(lambda x : (int(x.split(",")[0]), x.split(",")[1]))

# a function to compute broadcast join

def broadcast_join(first_file, second_file):
    # here the key is movie_id
    # we use broadcast and then collectAsMap to collect the RDD and create a mutable HashMap on the driver
    hashtable = sc.broadcast(second_file.map(lambda x : (x[0], x)). \
                groupByKey(). \
                collectAsMap())
    # we get the values from hashtable as a list that contains (movie_id, genre) tuples
    final = first_file.flatMap(lambda x : [(x[0], (r[1], x[1])) for r in hashtable.value.get(x[0], [])])
    return final
t1 = time.time()
RDD.broadcast_join = broadcast_join

final = broadcast_join(first_file, second_file). \
        map(lambda x : (x[0], x[1][0], x[1][1][0], x[1][1][1], x[1][1][2], x[1][1][3])) #. \
        # collect()


# for i in final:
#     print(i)

def toCSVLine(data):
  return ','.join(str(d) for d in data)
lines = final.map(toCSVLine)

final.saveAsTextFile('hdfs://master:9000/outputs/broadcast_join.csv')
t2 = time.time()
a_file = open("times.json", "r")
a_dictionary = json.load(a_file)
a_dictionary.update({"broadcast_join" : t2-t1})

a_file = open("times.json", "w")
a_file = json.dump(a_dictionary, a_file)

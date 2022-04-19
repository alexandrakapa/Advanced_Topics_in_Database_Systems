from pyspark.sql import SparkSession
import time
from pyspark.sql.functions import broadcast
import json
spark = SparkSession.builder.appName("query5-rdd").getOrCreate()
import csv

from io import StringIO

sc = spark.sparkContext


def split_complex(x): return list(csv.reader(StringIO(x),delimiter=','))[0]

def to_list(a):
     return [a]

def append(a, b):
        if(a[0][1] < b[1]): return [b]
        if (a[0][1] == b[1]):
                a.append(b)
                return a
        if (a[0][1] > b[1]):return a


def extend(a, b):
        if(a[0][1] < b[0][1]): return b
        if (a[0][1] == b[0][1]):
                a.extend(b)
                return a
        if (a[0][1] > b[0][1]):return a

t1 = time.time()
movies = sc.textFile("hdfs://master:9000/files/movies.csv"). \
        map(lambda x : (int(split_complex(x)[0]),(split_complex(x)[1],float(split_complex(x)[7]))))

categories = sc.textFile("hdfs://master:9000/files/movie_genres.csv"). \
        map(lambda x : (int(x.split(",")[0]),x.split(",")[1]))



ratings = sc.textFile("hdfs://master:9000/files/ratings.csv").map(lambda x:(int(x.split(",")[1]),(int(x.split(",")[0]),float(x.split(",")[2]))))

rati = sc.textFile("hdfs://master:9000/files/ratings.csv").map(lambda x:(int(x.split(",")[1]),int(x.split(",")[0])))




user_rat = ratings.join(movies). \
        map(lambda x : (x[0], (x[1][0][0], x[1][1][0], x[1][0][1], x[1][1][1]))). \
	join(categories). \
	map(lambda x : ((x[1][1], x[1][0][0]), (x[1][0][1], x[1][0][2], x[1][0][3])))

best_user = \
        user_rat.reduceByKey(lambda x, y: x if x[1] > y[1] or (x[1] == y[1] and x[2] > y[2]) else y)

worst_user = \
        user_rat.reduceByKey(lambda x, y: x if x[1] < y[1] or (x[1] == y[1] and x[2] > y[2]) else y)

def f(x): return x
user_per = categories.join(rati).map(lambda x: (x[1],1)).reduceByKey(lambda x,y:x+y).map(lambda x: (x[0][0],(x[0][1],x[1]))).combineByKey(to_list, append, extend).flatMapValues(f).map(lambda x:((x[0],x[1][0]),x[1][1]))


res = best_user.join(worst_user).map(lambda x:(x[0], (x[1][0][0],x[1][0][1],x[1][1][0],x[1][1][1]))).join(user_per).sortBy(lambda x: x[0],ascending = True)


def toCSVLine(data):
  return ','.join(str(d) for d in data)
lines = res.map(toCSVLine)

lines.saveAsTextFile('hdfs://master:9000/outputs/Q5_RDD.csv')
t2 = time.time()

a_file = open("times.json", "r")
a_dictionary = json.load(a_file)
a_dictionary.update({"q5-rdd" : t2-t1})

a_file = open("times.json", "w")
a_file = json.dump(a_dictionary, a_file)

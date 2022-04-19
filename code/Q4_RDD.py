from pyspark.sql import SparkSession
import json
spark = SparkSession.builder.appName("query3-rdd").getOrCreate()
import time
import csv
from io import StringIO
def split_complex(x): return list(csv.reader(StringIO(x),delimiter=','))[0]
def count_udf(s_in):return(len(s_in.split()))
spark = SparkSession.builder.appName("Q4-RDD").getOrCreate()


def year_culc(y_in):
	y_in = int(y_in[0:4])
	if(y_in >=2000 and y_in <= 2004):return "2000-2004"
	elif(y_in>=2005 and y_in <=2009):return "2005-2009"
	elif(y_in>=2010 and y_in <=2014):return "2010-2014"
	elif(y_in >= 2015 and y_in<=2019):return "2015-2019"
	elif(y_in >=2020 and y_in <=2024):return "2020-2024"

sc = spark.sparkContext
spark.udf.register("count_word",count_udf)
t1 = time.time()
movies  = sc.textFile("hdfs://master:9000/files/movies.csv"). \
	map(lambda x : (int(split_complex(x)[0]),(count_udf(split_complex(x)[2]),split_complex(x)[3]))).filter(lambda x: x[1][0]!=0).filter(lambda x: x[1][1] != ''). \
	filter(lambda x: int((x[1][1].split("-"))[0]) >=2000)

categories = sc.textFile("hdfs://master:9000/files/movie_genres.csv"). \
	map(lambda x : (int(x.split(",")[0]),x.split(",")[1])).filter(lambda x: x[1] == "Drama")


res = movies.join(categories). \
	map(lambda x: (year_culc(x[1][0][1]),(x[1][0][0],1))).reduceByKey(lambda a,b: (a[0] + b[0], a[1] + b[1])).map(lambda x: ( x[0], x[1][0]/x[1][1]))

def toCSVLine(data):
  return ','.join(str(d) for d in data)
lines = res.map(toCSVLine)

lines.saveAsTextFile('hdfs://master:9000/outputs/Q4_RDD.csv')
t2 = time.time()
print("\nExecution time:")
print("--- %s seconds ---" % '{:.20f}'.format(t2-t1))

a_file = open("times.json", "r")
a_dictionary = json.load(a_file)
a_dictionary.update({"q4-rdd" : t2-t1})

a_file = open("times.json", "w")
a_file = json.dump(a_dictionary, a_file)

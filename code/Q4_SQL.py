from pyspark.sql import SparkSession
import math
import sys
import time
import json
spark = SparkSession.builder.appName("query4-sql").getOrCreate()


file_format = sys.argv[1]

if file_format == 'parquet' :
    movies = spark.read.parquet("hdfs://master:9000/files/movies.parquet")
    categories  = spark.read.parquet("hdfs://master:9000/files/movie_genres.parquet")
elif file_format == 'csv' :
    movies = spark.read.format("csv").options(header = 'false',inferSchema='true').load("hdfs://master:9000/files/movies.csv")
    categories = spark.read.format("csv").options(header ='false',inferSchema='true').load("hdfs://master:9000/files/movie_genres.csv")

def ret_year(s_ye):
	if(s_ye == 400):return "2000-2004"
	elif(s_ye == 401):return "2005-2009"
	elif(s_ye ==402):return "2010-2014"
	elif(s_ye ==403): return "2015-2019"


def count_udf(s_in):
	return(len(s_in.split()))

def year_ret(y_in):
	if(y_in>=2000 and y_in <=2004): return 0
	if(y_in >= 2005 and y_in <=2009): return 1
	if(y_in >=2010 and y_in <=2014):return 2
	if(y_in >= 2015 and  y_in <=2019 ): return 3
	if(y_in >=2020 and y_in <=2024): return 4



movies.registerTempTable("movies")
categories.registerTempTable("categories")

spark.udf.register("count_word",count_udf)
spark.udf.register("year_ret",year_ret)
spark.udf.register("return_year",ret_year)
sqlString = "select avg(count_word(m._c2)),return_year(YEAR(m._c3) DIV 5)  from movies as m, categories as c where m._c0 == c._c0 and m._c2 !='' and (YEAR(m._c3) >= 2000) and c._c1 = 'Drama' group by (YEAR(m._c3) DIV  5) order by (YEAR(m._c3) DIV 5) asc"

t1 = time.time()
res = spark.sql(sqlString)

if file_format == 'parquet':
    nam = "q4-sql-parquet"
    namef = "hdfs://master:9000/outputs/"+nam+".csv"
elif file_format== "csv":
    nam = "q4-sql-csv"
    namef = "hdfs://master:9000/outputs/"+nam+".csv"
t2 = time.time()

res.coalesce(1).write.format('csv').save(namef)
print("\nExecution time:")
print("--- %s seconds ---" % '{:.20f}'.format(t2-t1))

a_file = open("times.json", "r")
a_dictionary = json.load(a_file)
a_dictionary.update({nam : t2-t1})

a_file = open("times.json", "w")
a_file = json.dump(a_dictionary, a_file)

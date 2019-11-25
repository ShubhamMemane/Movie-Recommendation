from pyspark import SparkConf
from  pyspark import SparkContext
from pyspark.sql import SQLContext
from pyspark.sql import SparkSession
from pyspark.sql.functions import *

conf = SparkConf().setAppName("Demo1").setMaster("local")
sc = SparkContext(conf = conf)
sqlContext = SQLContext(sc)

spark = SparkSession.builder\
    .config("spark.sql.shuffle.partitions", 2)\
    .appName("demo05")\
    .master("local[2]")\
    .getOrCreate()

ratings_schema = "userId INT, movieId INT, rating DOUBLE, rating_time BIGINT"
movies_schema = "movieId INT, title STRING, genres STRING"


movies = spark.read\
    .schema(movies_schema)\
    .option("header", "true")\
    .option("delimiter", ",")\
    .csv("/home/sunbeam/big-data/data/movies/movies.csv")

ratings= spark.read\
    .schema(ratings_schema)\
    .option("header", "true")\
    .option("delimiter", ",")\
    .csv("/home/sunbeam/big-data/data/movies/ratings.csv")

step1 = sqlContext.registerDataFrameAsTable(ratings,"r1")

one = spark.sql("select a.movieId m1, b.movieId m2, a.rating rat1, b.rating rat2 from  r1 a join   r1 b on (a.userId = b.userId) where a.movieId < b.movieId ")

step2 = sqlContext.registerDataFrameAsTable(one,"r2")

two =  spark.sql("SELECT m1,m2,ROUND(corr(rat1,rat2),2) as cor,COUNT(rat1) as cnt from r2 group by m1,m2  having cnt >1 order by cor desc")

step3 = sqlContext.registerDataFrameAsTable(two,"corre")

step4 =sqlContext.registerDataFrameAsTable(movies,"movie")

#three = spark.sql("select * from movie")

four= spark.sql("select m.title recommendation from movie m inner join  corre c on m.movieId in (c.m1,c.m2) where 11 in (c.m1,c.m2 ) AND c.cor > 0.5 AND c.cnt > 40 AND m.title != (SELECT title FROM movie WHERE movieId = 11)")

#three.show(truncate=False)
four.show(truncate=False)



spark.stop()
sc.stop()

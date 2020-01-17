from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *

conf = SparkConf()
conf.setAppName('Movies')
conf.setMaster('local')

sc = SparkContext(conf=conf)

u_path = '/home/mehul/Documents/corporatetraining/data-analysis-data/movielens/users.dat'
r1 = sc.textFile(u_path)
r2 = r1.map(lambda line: line.split('::'))
r3 = r2.map(lambda tokens: (int(tokens[0]), tokens[1], int(tokens[2]), int(tokens[3]), int(tokens[4])))

spark = SparkSession.builder\
    .appName('Movies')\
    .master('local')\
    .getOrCreate()

u_fields = [
    StructField('user_id', IntegerType(), True),
    StructField('gender', StringType(), True),
    StructField('age', IntegerType(), True),
    StructField('occu', IntegerType(), True),
    StructField('postal', IntegerType(), True)
]
u_schema = StructType(u_fields)
users = spark.createDataFrame(r3, schema=u_schema)

m_path = '/home/mehul/Documents/corporatetraining/data-analysis-data/movielens/movies.dat'
r4 = sc.textFile(m_path)
r5 = r4.map(lambda line: line.split('::'))
r6 = r5.map(lambda tokens: (int(tokens[0]), tokens[1], tokens[2]))

m_fields = [
    StructField('movie_id', IntegerType(), True),
    StructField('title', StringType(), True),
    StructField('genres', StringType(), True)
]
m_schema = StructType(m_fields)
movies = spark.createDataFrame(r6, schema=m_schema)



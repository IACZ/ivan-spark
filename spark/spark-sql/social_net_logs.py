from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *

spark = SparkSession.builder\
    .appName('social net. logs')\
    .master('local')\
    .getOrCreate()

fields = [
    StructField('username', StringType(), False),
    StructField('date', DateType(), False),
    StructField('time', StringType(), False),
    StructField('action', StringType(), False)
]
schema = StructType(fields)

path = '/home/mehul/Documents/corporatetraining/data-analysis-data/facebook_logs'

d1 = spark.read.csv(path, schema=schema)
d2 = d1.select(d1.username, d1.action)
d3 = d2.where(d2.action == 'out')
d4 = d3.groupBy(d3.username)
d5 = d4.count()
d6 = d5.orderBy(desc('count'))

d6.write.csv('/home/mehul/Desktop/social_logs_sparksql')
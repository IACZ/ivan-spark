from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *

spark = SparkSession.builder\
    .appName('distinct words')\
    .master('local')\
    .getOrCreate()

path = '/home/mehul/Documents/corporatetraining/hadoop_required_files_neebal/word_count_input'
d1 = spark.read.text(path)
d2 = d1.select(d1.value.alias('line'))
d3 = d2.select(explode(split('line', ' ')).alias('words'))
d4 = d3.groupBy(d3.words)
d5 = d4.count()
d6 = d5.orderBy(desc('count'))

d6.write.csv('/tmp/word_count_spark_sql')
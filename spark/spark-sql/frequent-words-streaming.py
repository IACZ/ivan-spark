from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *

spark = SparkSession.builder\
    .appName('frequent words streaming')\
    .master('local')\
    .getOrCreate()

lines = spark.readStream\
    .format('socket')\
    .option('host', 'localhost')\
    .option('port', 9879)\
    .load()

words_df = lines.select(explode(split(lines.value, ' ')).alias('words'))
count_df = words_df.groupBy('words')\
    .count()\
    .orderBy(desc('count'))

q = count_df.writeStream\
    .format('console')\
    .outputMode('complete')\
    .start()
q.awaitTermination()
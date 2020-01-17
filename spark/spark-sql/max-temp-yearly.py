from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *

spark = SparkSession.builder\
    .appName('max temp yearly')\
    .master('local')\
    .getOrCreate()

fields = [
    StructField('station', IntegerType(), False),
    StructField('year', IntegerType(), False),
    StructField('datemonth', StringType(), False),
    StructField('hourminute', StringType(), False),
    StructField('temperature', FloatType(), False)
]
schema = StructType(fields)

path = '/home/mehul/Documents/corporatetraining/hadoop_required_files_neebal/weather_data_input'

d1 = spark.read.csv(path, schema=schema, sep='|')
d2 = d1.dropna()
d3 = d2.select(d2.year, d2.temperature)
d4 = d3.groupBy(d3.year)
d5 = d4.agg(max('temperature').alias('maxTemperature'))

d5.write.csv('/home/mehul/Desktop/max-temp-yearly')

spark.stop()

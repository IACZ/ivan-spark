from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *

spark = SparkSession.builder\
    .appName('matrix mul')\
    .master('local')\
    .getOrCreate()

m_path = '/home/mehul/Documents/corporatetraining/hadoop_required_files_neebal/matrix_input/m_matrix'
m_fields = [
    StructField('m_r', IntegerType(), False),
    StructField('m_c', IntegerType(), False),
    StructField('m_v', IntegerType(), False)
]
m_schema = StructType(m_fields)
m = spark.read.csv(m_path, schema=m_schema)

n_path = '/home/mehul/Documents/corporatetraining/hadoop_required_files_neebal/matrix_input/n_matrix'
n_fields = [
    StructField('n_r', IntegerType(), False),
    StructField('n_c', IntegerType(), False),
    StructField('n_v', IntegerType(), False)
]
n_schema = StructType(n_fields)
n = spark.read.csv(n_path, schema=n_schema)

j = m.join(n, m.m_c == n.n_r)
cross_product = j.select(j.m_r, j.n_c, (j.m_v * j.n_v).alias('cross'))
group_data = cross_product.groupBy(cross_product.m_r, cross_product.n_c)
ans = group_data.agg(sum('cross').alias('v'))

ans.write.csv('/tmp/matrix_output')
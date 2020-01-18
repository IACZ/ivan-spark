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
r3 = r2.map(lambda tokens: (int(tokens[0]), tokens[1], int(tokens[2]), int(tokens[3]), tokens[4]))

spark = SparkSession.builder\
    .appName('Movies')\
    .master('local')\
    .getOrCreate()

u_fields = [
    StructField('user_id', IntegerType(), True),
    StructField('gender', StringType(), True),
    StructField('age', IntegerType(), True),
    StructField('occu', IntegerType(), True),
    StructField('postal', StringType(), True)
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

r_path = '/home/mehul/Documents/corporatetraining/data-analysis-data/movielens/ratings.dat'
r7 = sc.textFile(r_path)
r8 = r7.map(lambda line: line.split('::'))
r9 = r8.map(lambda tokens: (int(tokens[0]), int(tokens[1]), int(tokens[2]), int(tokens[3])))

r_fields = [
    StructField('user_id', IntegerType(), True),
    StructField('movie_id', IntegerType(), True),
    StructField('rating', IntegerType(), True),
    StructField('timestamp', IntegerType(), True)
]
r_schema = StructType(r_fields)
ratings = spark.createDataFrame(r9, schema=r_schema)

# get the titles of the popular films (no of people who rated > 800)
ratings_movies = ratings.join(movies, ratings.movie_id == movies.movie_id)
popular_films = ratings_movies.groupBy(ratings_movies.title)\
    .count()\
    .where('count > 800')\
    .orderBy(desc('count'))
popular_titles = popular_films.select(popular_films.title.alias('p_title'))

# the mean ratings of the popular films by gender
popular_ratings = ratings_movies.join(popular_titles, ratings_movies.title == popular_titles.p_title)

d1 = popular_ratings.join(users, popular_ratings.user_id == users.user_id)
d2 = d1.select(d1.title, d1.gender, d1.rating)
d3 = d2.groupBy(d2.title)\
    .pivot('gender', ['M', 'F'])\
    .mean('rating')\
    .orderBy(desc('M'))

d3.write.csv('/tmp/movies_spark_sql')








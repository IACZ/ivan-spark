from pyspark import SparkConf, SparkContext

conf = SparkConf()
conf.setAppName('Distinct words')
conf.setMaster('local')

sc = SparkContext(conf=conf)
path = '/home/mehul/Documents/lectures/hadoop-kit/data/word_count_input'
r1 = sc.textFile(path)
r2 = r1.flatMap(lambda line: line.split(' '))
r3 = r2.map(lambda word: (word, 1))
r4 = r3.reduceByKey(lambda ele1, ele2: ele1 + ele2)
r5 = r4.sortBy(lambda entry: entry[1], ascending=False)

r5.saveAsTextFile('/home/mehul/Desktop/distinct-words')
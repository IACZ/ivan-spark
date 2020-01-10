from pyspark import SparkConf, SparkContext

conf = SparkConf()
conf.setAppName('Users and logouts')
conf.setMaster('local')

sc = SparkContext(conf=conf)
path = '/home/mehul/Documents/corporatetraining/data-analysis-data/facebook_logs'

r1 = sc.textFile(path)
r2 = r1.map(lambda line: line.split(','))
r3 = r2.map(lambda tokens: (tokens[0], tokens[-1]))
r4 = r3.filter(lambda entry: entry[-1] == 'out')
r5 = r4.groupByKey()
r6 = r5.map(lambda entry: (entry[0], len(entry[-1])))

r6.saveAsTextFile('/home/mehul/Desktop/social_logs')
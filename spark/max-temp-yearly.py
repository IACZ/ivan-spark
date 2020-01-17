from pyspark import SparkConf, SparkContext

conf = SparkConf()
conf.setAppName('Max temperature yearly')
conf.setMaster('local')

sc = SparkContext(conf=conf)
path = '/home/mehul/Documents/corporatetraining/data-analysis-data/weather_data_input'

r1 = sc.textFile(path)

r2 = r1.map(lambda line: line.split('|'))

r3 = r2.filter(lambda tokens: tokens[-1] != '' and tokens[-1] != 'NA')

r4 = r3.map(lambda tokens: (int(tokens[1]), float(tokens[-1])))

r5 = r4.reduceByKey(lambda temp1, temp2: max(temp1, temp2))

r5.saveAsTextFile('/home/mehul/Desktop/max-temp-yearly-spark')

sc.stop()
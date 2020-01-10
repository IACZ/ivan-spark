from mrjob.job import MRJob

class DistinctWords(MRJob):
    def mapper(self, key, value):
        tokens = value.split(' ')
        for token in tokens:
            yield token, 'W'

    def reducer(self, key, values):
        yield key, 'DW'

if __name__ == '__main__':
    DistinctWords.run()
from mrjob.job import MRJob
from mrjob.step import MRStep

class NoOfDistinctWords(MRJob):
    def steps(self):
        return [
            MRStep(mapper=self.mapper_word, reducer=self.reducer_distinct_words),
            MRStep(mapper=self.mapper_second_step, reducer=self.reducer_noof_distinct_words)
        ]

    def mapper_word(self, key, value):
        tokens = value.split(' ')
        for token in tokens:
            yield token, 'W'

    def reducer_distinct_words(self, key, values):
        yield key, 'DW'

    def mapper_second_step(self, key, value):
        yield 'DW', 1

    def reducer_noof_distinct_words(self, key, values):
        yield key, sum(values)

if __name__ == '__main__':
    NoOfDistinctWords.run()
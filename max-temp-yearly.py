from mrjob.job import MRJob

class MaxTempYearly(MRJob):
    def mapper(self, key, value):
        tokens = value.split('|')
        if len(tokens) == 5:
            try:
                year = int(tokens[1])
                temperature = float(tokens[-1])
            except:
                pass
            else:
                yield year, temperature

    def reducer(self, key, values):
        max_temp = max(values)
        yield key, max_temp

if __name__ == '__main__':
    MaxTempYearly.run() # help u run it locally
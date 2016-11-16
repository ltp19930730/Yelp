import nltk
from mrjob.job import MRJob

class MRWordFrequencyCount(MRJob):
    def mapper(self, _, line):
        # yield "chars", len(line)
        # yield "words", len(line.split())
        # yield "lines", 1
        is_noun = lambda pos:pos[:2] == 'NN'

        tokenized = nltk.word_tokenize(line)
        nouns = [word for (word, pos) in nltk.pos_tag(tokenized) if is_noun(pos)]
        for noun in nouns:
            yield noun, 1

    def reducer(self, key, values):
        yield key, sum(values)

if __name__ == '__main__':
    MRWordFrequencyCount.run()

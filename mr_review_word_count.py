from mrjob.job import MRJob
from mrjob.protocol import JSONValueProtocol
from mrjob.step import MRStep
import nltk

# choose a business to analysis
BUSINESS_ID = "lliksv-tglfUz1T3B3vgvA"
MINIMUM_OCCURENCES = 20
def words(text):
    for  word in text.split():
        # normalize words by lowercasing and dropping non-alpha
        # characters
        normed = re.sub('[^a-z]','',word.lower())
        if normed:
            yield normed

def dates(date):
    #split the date in to a map
    year, month, day = map(int, date.split('-'))
    if month in range(1,7):
        yield year,'season1'
    else:
        yield year,'season2'

class CountPeriodWord(MRJob):

    INPUT_PROTOCOL = JSONValueProtocol

    def review_period_mapper(self, _, data):
        # map the review text into a same period
        if data['business_id'] == BUSINESS_ID:
            years = dates(data['date'])
            for year,season in years:
                yield (year,season), data['text']

    def review_period_reducer(self, period, review_text):
        reviews = []
        for text in review_text:
            reviews.append(text)
        yield period, reviews

    def count_word_mapper(self, period, reviews):
        for review in reviews:
            is_noun = lambda pos:pos[:2] == 'NN'

            tokenized = nltk.word_tokenize(review)
            nouns = [word for (word, pos) in nltk.pos_tag(tokenized) if is_noun(pos)]
            for noun in nouns:
                yield period,(noun, 1)

    def count_word_reducer(self, period, word_count):
        raw_counts = {}
        for word, count in word_count:
            raw_counts[word] = raw_counts.get(word, 0) + count

        filtered_counts = {}
        for word, count in raw_counts.iteritems():
            if count >= MINIMUM_OCCURENCES:
                filtered_counts[word] = count
        filtered_counts = sorted(filtered_counts, key=lambda x:x[0])
        yield period, filtered_counts

    def steps(self):
        return [MRStep(mapper=self.review_period_mapper,
                reducer=self.review_period_reducer),
                MRStep(mapper=self.count_word_mapper,
                reducer=self.count_word_reducer),]


if __name__ == '__main__':
    CountPeriodWord.run()

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
    if month in range(1,4):
        yield year,'season1'
    elif month in range(4,7):
        yield year,'season2'
    elif month in range(7,10):
        yield year,'seaons3'
    else:
        yield year,'season4'


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
            exit_noun = set()
            tokenized = nltk.word_tokenize(review)
            nouns = [word for (word, pos) in nltk.pos_tag(tokenized) if is_noun(pos)]

            for noun in nouns:
                if noun == 'Nam':
                    yield period,review


    def count_word_reducer(self, period, reviews):
        for review in reviews:
            yield period,review

    def steps(self):
        return [MRStep(mapper=self.review_period_mapper,
                reducer=self.review_period_reducer),
                MRStep(mapper=self.count_word_mapper,
                reducer=self.count_word_reducer),]


if __name__ == '__main__':
    CountPeriodWord.run()

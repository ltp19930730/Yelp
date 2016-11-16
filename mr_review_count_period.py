from mrjob.job import MRJob
from mrjob.protocol import JSONValueProtocol


BUSINESS_ID = "lliksv-tglfUz1T3B3vgvA"

def words(text):
    for  word in text.split():
        # normalize words by lowercasing and dropping non-alpha
        # characters
        normed = re.sub('[^a-z]','',word.lower())

        if normed:
            yield normed


def dates(date):
    #split the date in to  a  map
    year, month, day = map(int, date.split('-'))
    if month in range(1,7):
        yield year,'season1'
    else:
        yield year,'season2'

class CountPeriod(MRJob):

    INPUT_PROTOCOL = JSONValueProtocol

    def mapper(self, _, data):
        if data['business_id'] == BUSINESS_ID:
            years = dates(data['date'])
            for year,season in years:
                yield (year,season), 1

    def reducer(self, year, review_count):
        yield year, sum(review_count)



if __name__ == '__main__':
    CountPeriod.run()

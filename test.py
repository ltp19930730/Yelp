from mrjob.job import MRJob
from mrjob.protocol import JSONValueProtocol

MINIMUM_REVIEWS = 1000
BUSINESS_ID = "lliksv-tglfUz1T3B3vgvA"

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

class ReviewCount(MRJob):

    INPUT_PROTOCOL = JSONValueProtocol

    def mapper(self, _, data):
        # map the review text into a same period
        if data['business_id'] == BUSINESS_ID:
            years = dates(data['date'])
            for year,season in years:
                yield (year,season), data['stars']

    def reducer(self, business_date, stars):
            num = 0
            star_total = 0
            for star in stars:
                star_total = star_total + star
                num = num + 1
            yield business_date,star_total*1.0/num


if __name__ == '__main__':
    ReviewCount.run()

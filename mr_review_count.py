import re

from mrjob.job import MRJob
from mrjob.step import MRStep
from mrjob.protocol import JSONValueProtocol

MINIMUM_REVIEWS = 1000


class ReviewCount(MRJob):

    INPUT_PROTOCOL = JSONValueProtocol

    def review_filter_mapper(self, _, data):
        if data['review_count'] > MINIMUM_REVIEWS:
            yield (data['name'],data['business_id']), data['review_count']

    def review_filter_reducer(self, business_id, review_count):
        yield business_id, sum(review_count)

    def steps(self):
        return [MRStep(mapper=self.review_filter_mapper,
                       reducer=self.review_filter_reducer)]
if __name__ == '__main__':
    ReviewCount.run()

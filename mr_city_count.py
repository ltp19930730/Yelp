from mrjob.job import MRJob
from mrjob.step import MRStep
from mrjob.protocol import JSONValueProtocol

class CityBusiness(MRJob):

    BUSINESS_ID = []

    INPUT_PROTOCOL = JSONValueProtocol

    def review_filter_mapper(self, _, data):
        if data['city'] == 'Las Vegas':
            yield data['city'], data['business_id']

    def review_filter_reducer(self, city, business_id):
        BUSINESS_ID = []

        for business in business_id:
            BUSINESS_ID.append(business)

        yield city,BUSINESS_ID

    def steps(self):
        return [MRStep(mapper=self.review_filter_mapper,
                       reducer=self.review_filter_reducer)]

if __name__ == '__main__':
    CityBusiness.run()

from mrjob.job import MRJob
from mrjob.step import MRStep
from mrjob.protocol import JSONValueProtocol
import json

# def load_business_id():

 # in city (Las Vegas)

# class countWordFreq(MRJob):
#
#     INPUT_PROTOCOL = JSONValueProtocol
#
#     BUSINESS_ID = []
#
#     def mapper(self, _, data):
#         if data['']













if __name__ == '__main__':
    config = json.loads(open('city_business.json').read())
    print(config)

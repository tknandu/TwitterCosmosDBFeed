from tweepy.streaming import StreamListener
from config import *
import json

class CosmosDBListener(StreamListener):
 
    def __init__(self, client, collLink):
        self.client = client
        self.collLink = collLink
        
    def on_data(self, data):
        try:
            dictData = json.loads(data)
            dictData["id"] = str(dictData["id"])
            self.client.CreateDocument(self.collLink, dictData)
            return True
        except BaseException as e:
            print("Error on_data: %s" % str(e))
        return True
 
    def on_error(self, status):
        print(status)
        return True
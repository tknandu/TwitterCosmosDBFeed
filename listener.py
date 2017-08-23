from tweepy.streaming import StreamListener
from config import *
import json

class CosmosDBListener(StreamListener):
 
    def __init__(self, client, collLink):
        self.client = client
        self.collLink = collLink
        self.logFile = open('logs.txt', 'a')
        
    def on_data(self, data):
        try:
            """
            with open('data.json', 'a') as f:
                f.write(data)
            """
            dictData = json.loads(data)
            dictData["id"] = str(dictData["id"])
            self.client.CreateDocument(self.collLink, dictData)
            return True
        except BaseException as e:
            self.logFile.write("Error on_data: %s" % str(e))
        return True
 
    def on_error(self, status):
        self.logFile.write(status)
        return True
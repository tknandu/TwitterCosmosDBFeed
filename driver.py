import tweepy
from tweepy import OAuthHandler
from config import *
from tweepy import Stream
from listener import CosmosDBListener

import pydocumentdb
from pydocumentdb import document_client
from pydocumentdb import documents
import datetime

if __name__ == '__main__':    
    auth = OAuthHandler(consumer_key, consumer_secret)
    auth.set_access_token(access_token, access_secret)
    api = tweepy.API(auth)

    connectionPolicy = documents.ConnectionPolicy()
    connectionPolicy.EnableEndpointDiscovery 
    connectionPolicy.PreferredLocations = preferredLocations

    client = document_client.DocumentClient(host, {'masterKey': masterKey}, connectionPolicy)
    dbLink = 'dbs/' + databaseId
    collLink = dbLink + '/colls/' + collectionId

    twitter_stream = Stream(auth, CosmosDBListener(client, collLink))
    twitter_stream.filter(track=['#CosmosDB', '#ApacheSpark', '#ChangeFeed', 'ChangeFeed', '#MachineLearning', '#BigData', '#DataScience', '#Mongo', '#Graph'], async=True)
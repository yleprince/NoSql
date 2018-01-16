# coding: utf-8
from pymongo import MongoClient
import pandas as pd
import sys

actor1 = str(sys.argv[1])
actor2 = str(sys.argv[2])
d1 = pd.to_datetime(sys.argv[3])
d2 = pd.to_datetime(sys.argv[4])

#ip_public_master = '184.72.120.138:27010'
#client = MongoClient(ip_public_master) # Add IP address
client = MongoClient()
db = client.gdelt
events = db.events
count = events.find({'Actor1Code' : actor1, 'Actor2Code' : actor2, 'Day' : {"$in":[d1, d2]}}).count()

print(str(actor2)+','+str(count))

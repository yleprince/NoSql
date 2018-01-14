# coding: utf-8
from twisted.web.static import File
from twisted.web.server import Site
from autobahn.twisted.websocket import WebSocketServerProtocol
import json
import os
from pymongo import MongoClient
import sys
import pandas as pd


client = MongoClient() # Add IP address
db = client.gdelt
events = db.events


def askMongo(Actor1, Date1, Date2):
    """Requests the MongoDb cluster AWS """
    list_Actors2 = events.distinct('Actor2Code')
    list_Actors2_clean=[]
    for pays in list_Actors2:
        if len(pays)<4:
            list_Actors2_clean.append(pays)

    dict_result={}
    for pays in list_Actors2_clean:
        dict_result[pays] = events.find({'Actor1Code' : Actor1, 'Actor2Code' : pays, 'Day' : {"$in":[pd.to_datetime(Date1),pd.to_datetime(Date2)]}}).count()
        dict_result

    return dict_result



class MyServerProtocol(WebSocketServerProtocol):
    def onConnect(self, request):
        print("Client connecting: {}".format(request.peer))

    def onOpen(self):
        print("WebSocket connection open.")

    def onMessage(self, payload, isBinary):
        if isBinary:
            print("Binary message received: {} bytes".format(len(payload)))
        else:
            msg = handle_msg(payload)
            if msg:
                self.sendMessage(msg.encode('utf8'), False)

    def onClose(self, wasClean, code, reason):
        print("WebSocket connection closed: {}".format(reason))


def handle_msg(msg):
    request = json.loads(msg.decode('utf8'))
    #print("Text message received")
    print(str(request))

    country = request["country"]
    startDate = str(request["start"])
    endDate = str(request["end"])

    mongoResult = askMongo(country, startDate, endDate)
    return json.dumps(mongoResult)


if __name__ == '__main__':
    import sys

    # static file server seving index_old.html as root
    root = File(".")

    from twisted.python import log
    from twisted.internet import reactor

    log.startLogging(sys.stdout)

    from autobahn.twisted.websocket import WebSocketServerFactory

    factory = WebSocketServerFactory()
    factory.protocol = MyServerProtocol

    reactor.listenTCP(9000, factory)
    site = Site(root)
    reactor.listenTCP(8080, site)
    reactor.run()

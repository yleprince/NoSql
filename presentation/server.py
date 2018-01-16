# coding: utf-8
from twisted.web.static import File
from twisted.web.server import Site
from autobahn.twisted.websocket import WebSocketServerProtocol
import json
import os
from pymongo import MongoClient
import sys
import pandas as pd
from multiprocessing import Pool

Actor1 = 'FRA'
d1 = pd.to_datetime("20171227")
d2 = pd.to_datetime("20171229")

def askMongo(Actor1, Date1, Date2):
    """Requests the MongoDb cluster AWS """
    actor2list = [a for a in events.distinct('Actor2Code') if len(a)<4 ]

    dict_result={}

    for pays in actor2list[:5]:
        dict_result[pays] = events.find({'Actor1Code' : Actor1, 'Actor2Code' : pays, 'Day' : {"$in":[pd.to_datetime(Date1),pd.to_datetime(Date2)]}}).count()
        print(pays, dict_result[pays])
    return dict_result

def getCountrieslist():
    ip_public_master = '184.72.120.138:27010'
    client = MongoClient(ip_public_master) # Add IP address
    #client = MongoClient()
    db = client.gdelt
    events = db.events
    actor2list = [a for a in events.distinct('Actor2Code') if len(a)<4 ]
    return actor2list

def request(a2):
    ip_public_master = '184.72.120.138:27010'
    client = MongoClient(ip_public_master) # Add IP address
    #client = MongoClient()
    db = client.gdelt
    events = db.events
    count = events.find({'Actor1Code' : Actor1, 'Actor2Code' : a2, 'Day' : {"$in":[d1, d2]}}).count()
    print(a2, count)
    return (a2, count)


def askMongoMulti(Actor1Code, Date1, Date2):
    """Requests the MongoDb cluster AWS """

    global Actor1    # Needed to modify global copy of globvar
    Actor1 = Actor1Code

    global d1    # Needed to modify global copy of globvar
    d1 = pd.to_datetime(Date1)
    global d2    # Needed to modify global copy of globvar
    d2 = pd.to_datetime(Date2)


    #actor2list = [a if len(a)<4 else a[:3] for a in events.distinct('Actor2Code')]

    #actor2list = getCountrieslist()
    actor2list = ['RUS', 'CAD', 'USA', 'CHI', 'BRA', 'AUS', 'KAZ',
    'IND', 'ARG', 'ALG', 'DAN', 'GRL', 'MEX', 'FRA', 'ANG', 'SAU',
    'IDN', 'SAU', 'COD', 'SOU', 'LIB', 'IRA', 'IRQ', 'YEM', 'TUR',
    'ITA', 'ESP', 'GRB', 'DEU', 'SWE', 'NOR', 'POL', 'ZAF', 'BOL',
    'CHL', 'DZA', 'EGY', 'MAU', 'JPN']

    frequencies = {country: 0 for country in actor2list}


    p = Pool(5)

    frequencies_list = list(p.map(request, actor2list[:100]))

    for f in frequencies_list:
        frequencies[f[0]] = f[1]

    return frequencies



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

    mongoResult = askMongoMulti(country, startDate, endDate)
    print(mongoResult)
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

import numpy as np
import pandas as pd
from multiprocessing import Pool
import re
import requests
import sys

from pymongo import MongoClient

url = "http://data.gdeltproject.org/events"

events_heads = ["GlobalEventID",  "Day", "MonthYear",    "Year",   "FractionDate",   "Actor1Code", "Actor1Name",
                "Actor1CountryCode", "Actor1KnownGroupCode",  "Actor1EthnicCode", "Actor1Religion1Code",
                "Actor1Religion2Code", "Actor1Type1Code", "Actor1Type2Code", "Actor1Type3Code", "Actor2Code",
                "Actor2Name", "Actor2CountryCode", "Actor2KnownGroupCode", "Actor2EthnicCode", "Actor2Religion1Code",
                "Actor2Religion2Code", "Actor2Type1Code", "Actor2Type2Code", "Actor2Type3Code", "IsRootEvent",
                "EventCode", "EventBaseCode", "EventRootCode", "QuadClass", "GoldsteinScale", "NumMentions",
                "NumSources", "NumArticles", "AvgTone", "Actor1Geo_Type", "Actor1Geo_Fullname",
                "Actor1Geo_CountryCode", "Actor1Geo_ADM1Code", "Actor1Geo_ADM2Code", "Actor1Geo_Lat", "Actor1Geo_Long",
                "Actor1Geo_FeatureID", "Actor2Geo_Type", "Actor2Geo_Fullname", "Actor2Geo_CountryCode",
                "Actor2Geo_ADM1Code", "Actor2Geo_ADM2Code", "Actor2Geo_Lat", "Actor2Geo_Long", "Actor2Geo_FeatureID",
                "ActionGeo_Type", "ActionGeo_Fullname", "ActionGeo_CountryCode", "ActionGeo_ADM1Code",
                "ActionGeo_ADM2Code", "Action2Geo_Lat", "Action2Geo_Long", "Action2Geo_FeatureID", "DATEADDED",
                "SOURCEURL"]


def getTextFromUrl(url):
    res = requests.get(url)
    if res.status_code == 200:
        return res.text

def extractFileList(rawHTML):
    regexZIP = re.compile(">(2017[0-9]{4}\.export\.CSV\.zip)<\/A>")
    filelist = regexZIP.findall(rawHTML)

    zipFiles = []
    seen = set()

    for file in filelist:
        if not file in seen:
            zipFiles.append(file)
            seen.add(file)

    return zipFiles

def actorCounter(file):
    file_path = url + "/" + file
    df = pd.read_csv(file_path, sep='\t', header=None, names=events_heads, dtype='str')
    occurences = df.Actor1Name.value_counts()[actorTested]
    print(occurences)
    return occurences

def getZipContent(filename):
    file_path = url + "/" + filename
    df = pd.read_csv(file_path, sep='\t', header=None, names=events_heads, dtype='str')
    return df



#Table de transfert entre les noms de pays et les codes correspondants:
url_geo_codes = "http://www.geonames.org/countries/"

dfgeo = pd.read_html(url_geo_codes)[1]
dfgeo.columns = dfgeo.iloc[0]
dfgeo = dfgeo[['ISO-3166alpha3', 'Country']][1:]

country_codes = set(dfgeo['ISO-3166alpha3'])


#On construit le DF qui contient toutes les lignes de GDELT où actor1GeoCode == 'FR'.
#Pour le moment on ne prend qu'un nombre restreint de jours.

rawHTML = getTextFromUrl(url)
filelist = extractFileList(rawHTML)

#Number of days to upload:
days = 10
print("filelist[:days] : ", filelist[:days])
p = Pool(5)
dfCODE = pd.concat(list(p.map(getZipContent, filelist[:days])), ignore_index=True)

isInCountryCodes = lambda code: True if code in country_codes else False
dfCODE_filtered0 = dfCODE[dfCODE.Actor1CountryCode.apply(isInCountryCodes)]
dfCODE_filtered = dfCODE_filtered0[dfCODE.Actor2CountryCode.apply(isInCountryCodes)]


# MONGO
client = MongoClient()
db = client.gdelt
events = db.events

data = dfCODE_filtered.T.to_dict().values()
result = events.insert_many(data)

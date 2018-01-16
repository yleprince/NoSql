# coding=utf-8
from multiprocessing import Pool
import numpy as np
import pandas as pd
import pprint
import pymongo
import re
import requests
import sys
import warnings
warnings.filterwarnings('ignore')
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

    newfilelist = []
    seen = set()

    for file in filelist:
        if not file in seen:
            newfilelist.append(file)
            seen.add(file)

    return newfilelist

def actorCounter(file):
    file_path = url + "/" + file
    df = pd.read_csv(file_path, sep='\t', header=None, names=events_heads, dtype='str')
    occurences = df.Actor1Name.value_counts()[actorTested]
    print(occurences)
    return occurences

def getActorCODELines(filename):
    """ On peut ameliorer en ne prenant que les lignes ou les acteurs 1 et 2 sont effectivement des pays"""
    #code = 'FRA'
    file_path = url + "/" + filename
    df = pd.read_csv(file_path, sep='\t', header=None, names=events_heads, dtype='str')
    #return df[df.Actor1Code == code]

    isInCountryCodes = lambda code: True if code in country_codes else False
    df_filt0 = df[df.Actor1CountryCode.apply(isInCountryCodes)]
    df_filt1 = df_filt0[df_filt0.Actor2CountryCode.apply(isInCountryCodes)]

    df_small = df_filt1[["Actor1Code", "Actor2Code", "Day"]]

    convert = lambda date: pd.to_datetime(date)
    tmp = df_small['Day'].apply(convert)
    df_small['Day'] = tmp
    print(filename[:8],df_small.shape[0])
    insertDF(df_small)


def insertDF(df):
    # MONGO
    client = MongoClient()
    db = client.gdelt
    events = db.events

    data = df.T.to_dict().values()
    result = events.insert_many(data)

#Table de transfert entre les noms de pays et les codes correspondants:
url_geo_codes = "http://www.geonames.org/countries/"

dfgeo = pd.read_html(url_geo_codes)[1]
dfgeo.columns = dfgeo.iloc[0]
dfgeo = dfgeo[['ISO-3166alpha3', 'Country']][1:]

countries = [country for country in dfgeo['ISO-3166alpha3'] if len(country)<4]
country_codes = set(countries)


#On construit le DF qui contient toutes les lignes de GDELT où actor1GeoCode == 'FR'.
#Pour le moment on ne prend qu'un nombre restreint de jours.

rawHTML = getTextFromUrl(url)
filelist = extractFileList(rawHTML)

#Nombre de jour utilise:


p = Pool(5)
p.map(getActorCODELines, filelist)

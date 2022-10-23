
from pymongo import MongoClient
import pandas as pd
import json
import os
import config
import time
import ingestion as ing
import datetime
import isodate
import config
import db_handling as dbh


if __name__ == "__main__":
    
    
    client = dbh.connect()
    print('Connected!')
    
    mydb = client["db"]
    mycol_stations = mydb["stations"]

    # query stations  (Meschede: 59872)
    # uuid, name, brand, street, house_number, post_code, city,latitude, longitude
    myquery = { "post_code": config.query["post_code"] }
    mydoc = mycol_stations.find(myquery)
    counter = 0
    stations ={}
    for x in mydoc:
        counter+=1
        print(f'{x["street"]}  {x["house_number"]}')
        stations[x["uuid"]] = x["uuid"]
    
    print(f'Number of statisons: {counter}')
    #print(stations)
    

    
    # query date range
    col_prices = mydb["prices"]
    list_uuid = []
    start = datetime.datetime(config.query["year_start"],
                              config.query["month_start"],
                              config.query["day_start"],
                              config.query["hour_start"],
                              config.query["minute_start"],
                              config.query["second_start"])
    
    end = datetime.datetime(config.query["year_end"],
                              config.query["month_end"],
                              config.query["day_end"],
                              config.query["hour_end"],
                              config.query["minute_end"],
                              config.query["second_end"])
    
    query = {'date': {'$gte': start, '$lte': end}}
    
    # Specify here which variables are to be saved
    # date, station_uuid, diesel, e5, e10, dieselchange, e5change, e10chang  (0=keine Änderung, 1=Änderung, 2=Entfernt, 3=Neu)
    mask = {"_id":1,"station_uuid":1}
    prices = col_prices.find(query,mask)
    counter = 0
    for x in prices:
        list_uuid.append(x["station_uuid"])
        print(x) 
        counter+=1
        print('') 


 
    print("--- end query ---")
    
    
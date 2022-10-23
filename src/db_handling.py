from pymongo import MongoClient
import pandas as pd
import json
import os
import config
import time
import ingestion as ing
import query
import datetime

# create database connection from given credentials and return db client
def connect(host='localhost', port=8082, user='root', password='example'):
    con_string = 'mongodb://{user}:{password}@{host}:{port}/?authSource=admin&readPreference=secondary&directConnection=true&ssl=false'.format(user=user, password=password, host=host, port=port)
    return MongoClient(con_string)


def get_csv_files(path):
    print(__file__)
    all_files = os.listdir(path)    
    return list(filter(lambda f: f.endswith('.csv'), all_files))


def get_files_to_extension(path, extension,coll_name):
    file_paths = []
    os.walk(path)
    for root, dirs_list, files_list in os.walk(path):
        for file_name in files_list:
          if os.path.splitext(file_name)[-1] == extension:
            if ing.check_ingestion_config(file_name,coll_name):
                file_paths.append(os.path.join(root, file_name))
    return file_paths 


def insert_from_csv(client, csv_path, db_name, coll_name,convert_date=False):
    db = client[db_name]
    coll = db[coll_name]
    data = pd.read_csv(csv_path)
    d = data.to_dict(orient='records')
    if convert_date:
        for obj in d:
            obj['date'] = datetime.datetime.strptime(f'{obj["date"]}00','%Y-%m-%d %H:%M:%S%z')
    return coll.insert_many(d)


if __name__ == "__main__":
    
    from os.path import dirname, abspath
    # inserting price data
    price_path = dirname(dirname(abspath(__file__))) + '/data/tankerkoenig-data/prices'
    files = get_files_to_extension(price_path, '.csv', 'prices')
    client = connect()
    print('Connected!')
    ing.ingestion_handling(client, files, 'prices')
    
    ################################################################

    # inserting station data
    station_path = dirname(dirname(abspath(__file__))) + '/data/tankerkoenig-data/stations'
    files = get_files_to_extension(station_path, '.csv', 'stations')
    ing.ingestion_handling(client, files, 'stations')
    
    ################################################################
    
    # query 
    query.query_handling(client)
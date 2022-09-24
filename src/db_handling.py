from pymongo import MongoClient
import pandas as pd
import json
import os

# create database connection from given credentials and return db client
def connect(host='localhost', port=8082, user='root', password='example'):
    con_string = 'mongodb://{user}:{password}@{host}:{port}/?authSource=admin&readPreference=secondary&directConnection=true&ssl=false'.format(user=user, password=password, host=host, port=port)
    return MongoClient(con_string)


def get_csv_files(path):
    print(__file__)
    all_files = os.listdir(path)    
    return list(filter(lambda f: f.endswith('.csv'), all_files))


def get_files_to_extension(path, extension):
    file_paths = []
    os.walk(path)
    for root, dirs_list, files_list in os.walk(path):
        for file_name in files_list:
          if os.path.splitext(file_name)[-1] == extension:
             file_paths.append(os.path.join(root, file_name))
    return file_paths 


def insert_from_csv(client, csv_path, db_name, coll_name):
    db = client[db_name]
    coll = db[coll_name]
    coll.drop()
    data = pd.read_csv(csv_path)
    d = data.to_dict(orient='records')
    return coll.insert_many(d)


if __name__ == "__main__":
   from os.path import dirname, abspath 
   data_path = dirname(dirname(abspath(__file__))) + '/data'
   files = get_files_to_extension(data_path, '.csv')
   client = connect()
   print('Connected!')
   
   for f in files[:10]:
       insert_from_csv(client, f, 'db', 'prices')
   print('Inserted rows from csv!')

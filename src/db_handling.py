from pymongo import MongoClient
import pandas as pd
import os

# create database connection from given credentials and return db client
def connect(host='localhost', port=8082, user='root', password='example'):
    con_string = 'mongodb://{user}:{password}@{host}:{port}/?authSource=admin&readPreference=secondary&directConnection=true&ssl=false'.format(user=user, password=password, host=host, port=port)
    return MongoClient(con_string)


# get all files in given path and sub directories which match given file extension
def get_files_to_extension(path, extension):
    file_paths = []
    os.walk(path)
    for root, dirs_list, files_list in os.walk(path):
        for file_name in files_list:
          if os.path.splitext(file_name)[-1] == extension:
             file_paths.append(os.path.join(root, file_name))
    return file_paths 


# write csv file in given path to given collection in database
def insert_from_csv(client, csv_path, db_name, coll_name):
    db = client[db_name]
    coll = db[coll_name]
    coll.drop()

    db.create_collection(
        coll_name,
        timeseries = {
          "timeField": "date",
        }
    )

    data = pd.read_csv(csv_path).to_dict(orient='records')
    coll = db[coll_name]
    return coll.insert_many(data)


if __name__ == "__main__":
   from os.path import dirname, abspath 
   data_path = dirname(dirname(abspath(__file__))) + '/data'
   files = get_files_to_extension(data_path, '.csv')
   client = connect()
   print('Connected!')
   
   for f in files[:10]:
       insert_from_csv(client, f, 'db', 'prices')
   print('Inserted rows from csv!')

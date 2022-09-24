from pymongo import MongoClient
import pandas as pd
import json
import os

# create database connection from given credentials and return db client
def connect(host='localhost', port=8082, user='root', password='example'):
    con_string = 'mongodb://{user}:{password}@{host}:{port}/?authSource=admin&readPreference=secondary&directConnection=true&ssl=false'.format(user=user, password=password, host=host, port=port)
    return MongoClient(con_string)

def get_csv_files(path):
    all_files = os.listdir(path)    
    return list(filter(lambda f: f.endswith('.csv'), all_files))


def insert_from_csv(client, csv_path, db_name, coll_name):
    db = client[db_name]
    coll = db[coll_name]
    data = pd.read_csv(csv_path)
    d = data.to_dict(orient='records')
    coll.insert_many(d)
    #return coll.count()


if __name__ == "__main__":

    client = connect()
    print(client)
    print('Connected!')
    test_file = get_csv_files('/home/luca/ts-mongodb/data/2014/06')
    print(test_file[0])
    insert_from_csv(client, '/home/luca/ts-mongodb/data/2014/06/' + test_file[0], 'db', 'prices')
    print('Inserted rows from csv!')


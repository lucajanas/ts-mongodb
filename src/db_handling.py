from pymongo import MongoClient
import pandas as pd
import json

# create database connection from given credentials and return db client
def connect(db_url='localhost', port=8081):
    return MongoClient(db_url, port)


def insert_from_csv(client, csv_path, db_name, coll_name):
    db = client[db_name]
    coll = db[coll_name]
    data = pd.read_csv(csv_path)
    payload = json.loads(data.to_json(orient='records'))
    coll.remove()
    coll.insert(payload)
    return coll.count()


if __name__ == "__main__":
    connect()
    print('Connected!')

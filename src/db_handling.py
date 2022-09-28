from pymongo import MongoClient
import pandas as pd
import os


# create database connection from given credentials and return db client
def connect(host='localhost', port=8082, user='root', password='example'):
    con_string = 'mongodb://{user}:{password}@{host}:{port}/?authSource=admin&readPreference=secondary&directConnection=true&ssl=false'.format(
        user=user, password=password, host=host, port=port)
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


def create_timeseries_collection(db, coll_name, time_field='date'):

    db.create_collection(
        coll_name,
        timeseries={
            "timeField": time_field,
        }
    )


# write csv file in given path to given collection in database
def insert_from_csv(csv_path, db, coll_name):
    data = pd.read_csv(csv_path).to_dict(orient='records')
    coll = db[coll_name]
    return coll.insert_many(data)


if __name__ == "__main__":
    from os.path import dirname, abspath

    # reading and inserting price data
    price_path = dirname(dirname(abspath(__file__))) + '/data/prices'
    files = get_files_to_extension(price_path, '.csv')
    client = connect()
    print('Connected!')

    coll_name = 'prices'
    db = client['db']
    prices = db[coll_name]
    prices.drop()
    #create_timeseries_collection(db, coll_name)

    for f in files[:10]:
        insert_from_csv(f, db, coll_name)
    print('Inserted price data from csv!')

    ################################################################

    # reading and inserting price data
    station_path = dirname(dirname(abspath(__file__))) + '/data/stations'
    files = get_files_to_extension(station_path, '.csv')

    coll_name = 'stations'
    stations = db[coll_name]
    stations.drop()

    for f in files[:10]:
        insert_from_csv(client, f, db, coll_name)
    print('Inserted station data from csv!')

from pymongo import MongoClient
import pandas as pd

# create database connection from given credentials and return db client
def connect(db_url='localhost', port=8081):
    return MongoClient(db_url, port)

if __name__ == "__main__":
    connect()
    print('Connected!')

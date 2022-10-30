from pymongo import MongoClient
import datetime

# create database connection from given credentials and return db client
def connect(host='localhost', port=8082, user='root', password='example'):
    con_string = 'mongodb://{user}:{password}@{host}:{port}/?authSource=admin&readPreference=secondary&directConnection=true&ssl=false'.format(user=user, password=password, host=host, port=port)
    return MongoClient(con_string)

client = connect()
print(client)

db = client['db']
collection = db['prices']

#params end---
low = datetime.datetime(2015, 6, 4, 0, 0, 0) #start of resampling period [datetime]
high = datetime.datetime(2015, 6, 30, 23, 59, 59) #end of resampling period [datetime]
station = '3907c523-3831-4eaf-aec3-d88cc68f5b2e' #station to be resampled
frequency = 'day' #resampling frequency (available: 'second', 'minute', 'hour', 'day', 'week')
#params start---

intervals = {'second':1000, 'minute':60000, 'hour':3600000, 'day':86400000, 'week':604800000}
interval = intervals[frequency]

#get original time series
cursor1 = collection.find({'date':{'$gte': low, '$lt': high}, 'station_uuid':station}).sort("date", 1)
print('original time series')
for document in cursor1:
    print(document)

#pipeline1: determine lower bound lb
pipeline_bound = [
{"$match": {'date':{'$lt': low}, 'station_uuid':station}},
{"$sort": {"date":1}},
{"$group":{'_id':'$station_uuid', "diesel_lb":{"$last":"$diesel"}, "e5_lb":{"$last":"$e5"}, "e10_lb":{"$last":"$e10"}}},
]

#pipeline2: resampling
pipeline_resampling = [
    {"$match": {'date':{'$gte': low, '$lt': high}, 'station_uuid':station}},
    {'$group': {
     '_id':{
       '$subtract': ["$date", {
         '$mod': [{
           '$subtract': ["$date", low]
         }, interval]
       }]},
        'diesel':{'$last':'$diesel'},
        'e5':{'$last':'$e5'},
        'e10':{'$last':'$e10'}
    }},
    {'$densify':{'field':'_id', 'range':{'step':1, 'unit':frequency, 'bounds':[low, high]}}},
    {'$fill':{'output':{'diesel':{'method':'locf'},'e5':{'method':'locf'},'e10':{'method':'locf'}}}},
    {"$sort": {"_id":1}}
]

# determine prices at start of sampling period
cursor2 = collection.aggregate(pipeline_bound)
for document in cursor2:
    diesel_lb = document['diesel_lb']
    e5_lb = document['e5_lb']
    e10_lb = document['e10_lb']

# Resampling
cursor3 = collection.aggregate(pipeline_resampling)
print('resampled time series')
# insert lb values if necessary
for document in cursor3:
    if document['diesel'] is None:
        document['diesel'] = diesel_lb
    if document['e5'] is None:
        document['e5'] = e5_lb
    if document['e10'] is None:
        document['e10'] = e10_lb
    print(document)

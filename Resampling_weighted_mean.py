from pymongo import MongoClient
import datetime
from datetime import timedelta

# create database connection from given credentials and return db client
def connect(host='localhost', port=8082, user='root', password='example'):
    con_string = 'mongodb://{user}:{password}@{host}:{port}/?authSource=admin&readPreference=secondary&directConnection=true&ssl=false'.format(user=user, password=password, host=host, port=port)
    return MongoClient(con_string)

client = connect()
print(client)

db = client['db']
collection = db['prices']


#1: time series resampling
#params start---
low = datetime.datetime(2015, 6, 4, 0, 0, 0) #start of resampling period [datetime]
high = datetime.datetime(2015, 6, 30, 23, 59, 59) #end of resampling period [datetime]
station = '3907c523-3831-4eaf-aec3-d88cc68f5b2e' #station to be resampled
frequency = 'day' #resampling frequency (available: 'second', 'minute', 'hour', 'day', 'week')
#params end---

intervals = {'second':1000, 'minute':60000, 'hour':3600000, 'day':86400000, 'week':604800000}
interval = intervals[frequency]

#get original time series
cursor1 = collection.find({'date':{'$gte': low, '$lt': high}, 'station_uuid':station}).sort("date", 1)
print('original time series')
for document in cursor1:
    print(document)

#timestamp
start = datetime.datetime.now()

#pipeline1: determine lower bound lb
pipeline_bound = [
{"$match": {'date':{'$lt': low}, 'station_uuid':station}},
{"$sort": {"date":1}},
{"$group":{'_id':'$station_uuid', "diesel_lb":{"$last":"$diesel"}, "e5_lb":{"$last":"$e5"}, "e10_lb":{"$last":"$e10"}}}
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

#timestamp
end = datetime.datetime.now()
duration = end-start
print('Duration of resampling:')
print(duration)


#2: time weighted average of gas prices for given time interval
#params start---
interval_lb = datetime.datetime(2015, 6, 4, 0, 0, 0) #start of period [datetime]
interval_ub = datetime.datetime(2015, 6, 7, 23, 59, 59) #end of period [datetime]
station = '3907c523-3831-4eaf-aec3-d88cc68f5b2e' #station of interest
#params end---

#pipeline4: determine lower bound for prices lb
pipeline_bound = [
{"$match": {'date':{'$lt': interval_lb}, 'station_uuid':station}},
{"$sort": {"date":1}},
{"$group":{'_id':'$station_uuid', "diesel_lb":{"$last":"$diesel"}, "e5_lb":{"$last":"$e5"}, "e10_lb":{"$last":"$e10"}}}
]

#timestamp
start = datetime.datetime.now()

# determine prices at start of sampling period
cursor4 = collection.aggregate(pipeline_bound)
for document in cursor4:
    diesel_lb = document['diesel_lb']
    e5_lb = document['e5_lb']
    e10_lb = document['e10_lb']

pipeline_interval = [
{"$match": {'date':{'$gte': interval_lb, '$lt': interval_ub}, 'station_uuid':station}},
{"$sort": {"date":1}}
]

# calculate time weighted mean for prices
cursor5 = collection.aggregate(pipeline_interval)
previous_timestamp = interval_lb
previous_price = {'diesel':diesel_lb, 'e5':e5_lb, 'e10':e10_lb}
weighted_price = {'diesel':0, 'e5':0, 'e10':0}
#calculte weigthed price for each intervall between documents in time interval (documents must be in chronological order)
for document in cursor5:
    duration_timedelta = document['date'] - previous_timestamp
    duration = duration_timedelta.total_seconds()
    weighted_price['diesel'] += duration * previous_price['diesel']
    weighted_price['e5'] += duration * previous_price['e5']
    weighted_price['e10'] += duration * previous_price['e10']
    #update previous values
    previous_timestamp = document['date']
    previous_price = {'diesel':document['diesel'], 'e5':document['e5'], 'e10':document['e10']}
#add weighted price for last interval
duration_timedelta = interval_ub - previous_timestamp
duration = duration_timedelta.total_seconds()
weighted_price['diesel'] += duration*previous_price['diesel']
weighted_price['e5'] += duration * previous_price['e5']
weighted_price['e10'] += duration * previous_price['e10']
#calculate weighted mean
full_period_timedelta = interval_ub - interval_lb
full_period = full_period_timedelta.total_seconds()
weighted_mean = {'diesel':weighted_price['diesel']/full_period, 'e5':weighted_price['e5']/full_period, 'e10':weighted_price['e10']/full_period}
#print results
print(f'For period from {interval_lb} to {interval_ub}')
print('weighted mean diesel:')
print(weighted_mean['diesel'])
print('weighted mean e5:')
print(weighted_mean['e5'])
print('weighted mean e10:')
print(weighted_mean['e10'])

#timestamp
end = datetime.datetime.now()
duration = end-start
print('Duration of determining weighted mean:')
print(duration)


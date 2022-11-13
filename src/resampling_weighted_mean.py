from pymongo import MongoClient
import datetime
from datetime import timedelta
import config
from asyncio.windows_events import NULL
import json
import os
import psutil
import platform
import cpuinfo
import time

resampling_log = {
    'start_time': '',
    'end_time': ''
}

def log_resampling(type):
    if type == 'start':
        now = datetime.datetime.now()
        resampling_log['start_time'] = now.strftime("%Y-%m-%d %H:%M:%S")
        tmp = f'\n\n{"=" * 40} Start resampling on {resampling_log["start_time"]} {"=" * 40}\n\n'
        print(tmp)

    elif type == 'cursor2':
        now = datetime.datetime.now()
        resampling_log['cursor2_time'] = now.strftime("%Y-%m-%d %H:%M:%S")
        tmp = f'\n{"=" * 40} Done with Cursor2 on {resampling_log["cursor2_time"]} {"=" * 40}\n\n'
        print(tmp)

    elif type == 'end':
        now = datetime.datetime.now()
        resampling_log['end_time'] = now.strftime("%Y-%m-%d %H:%M:%S")
        tmp = f'\n{"=" * 40} Done with Cursor3 and finished resampling on {resampling_log["end_time"]} {"=" * 40}\n\n'
        print(tmp)

        # gest system info
        uname = platform.uname()
        cpufreq = psutil.cpu_freq()

        system_info = {
            'system': uname.system,
            'release': uname.release,
            'version': uname.version,
            'machine': uname.machine,
            'processor': uname.processor,
            'processor_detail': cpuinfo.get_cpu_info()["brand_raw"],
            'physical_cores': psutil.cpu_count(logical=False),
            'max_frequency': f'{cpufreq.max:.2f}Mhz'
        }

        f_ing_log = open('./log/resampling_log.json', 'r')
        # returns JSON object as a dictionary
        log_dic = json.load(f_ing_log)
        f_ing_log.close()

        # write all logs
        log_dic[resampling_log['start_time']] = {
            'system_info': system_info,
            'configuration': config.resampling,
            'log': resampling_log
        }

        # Opening JSON file for write
        f_ing_log = open('./log/resampling_log.json', 'w')
        json.dump(log_dic, f_ing_log)
        f_ing_log.close()

def resampling_handling(client):

    if config.control['start_resampling'] == False:
        print("Skip resampling")
        return

    db = client['db']
    collection = db['prices']

    start_year = config.resampling['start_year']
    start_month = config.resampling['start_month']
    start_day = config.resampling['start_day']
    start_hour = config.resampling['start_hour']
    start_minute = config.resampling['start_minute']
    start_second = config.resampling['start_second']
    end_year = config.resampling['end_year']
    end_month = config.resampling['end_month']
    end_day = config.resampling['end_day']
    end_hour = config.resampling['end_hour']
    end_minute = config.resampling['end_minute']
    end_second = config.resampling['end_second']
    low = datetime.datetime(start_year, start_month, start_day, start_hour, start_minute, start_second)
    high = datetime.datetime(end_year, end_month, end_day, end_hour, end_minute, end_second)
    station = config.resampling['station']
    frequency = config.resampling['frequency']

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

    #timestamp: start executing mongoDB operations
    log_resampling('start')

    # determine prices at start of sampling period
    cursor2 = collection.aggregate(pipeline_bound)
    for document in cursor2:
        diesel_lb = document['diesel_lb']
        e5_lb = document['e5_lb']
        e10_lb = document['e10_lb']

    # timestamp: done determining lower bounds
    log_resampling('cursor2')

    # Resampling
    cursor3 = collection.aggregate(pipeline_resampling)

    #timestamp: end executing mongoDB operations
    log_resampling('end')

    # print output
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

    interval_lb = low
    interval_ub = high
    station = config.resampling['station']

    #pipeline4: determine lower bound for prices lb
    pipeline_bound = [
    {"$match": {'date':{'$lt': interval_lb}, 'station_uuid':station}},
    {"$sort": {"date":1}},
    {"$group":{'_id':'$station_uuid', "diesel_lb":{"$last":"$diesel"}, "e5_lb":{"$last":"$e5"}, "e10_lb":{"$last":"$e10"}}}
    ]

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
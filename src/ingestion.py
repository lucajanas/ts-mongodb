
from pymongo import MongoClient
import pandas as pd
import json
import os
import config
import time
import db_handling as dbh

ingestion_log ={
    "batch_size":config.ingestion['progress_batch_size'],
    "elapsed_batch_time":[],
    "storage_size":[],
    "complete_elapsed_time":0
}


def create_timeseries_collection(db, coll_name, time_field='date',meta_field='station_uuid'):
    db.command('create', coll_name, 
               timeseries={ 'timeField': time_field, 'metaField': meta_field })
    


def check_ingestion_config(file_name,coll_name) -> bool:
    """Files must have this format: 2014-06-08-prices.csv"""
    if config.ingestion[f'type_{coll_name}'] == 'all':
        return True
    name_splitted = file_name.split('-')
    if name_splitted[0] == config.ingestion[f'year_{coll_name}']:
        if config.ingestion[f'type_{coll_name}'] == "month":
            if name_splitted[1] == config.ingestion[f'month_{coll_name}']:
                return True
            else:
                return False
        else:
            return True
    else:
        return False
    
    
    
def ingestion_handling(client,files,coll_name):
    
    start_time_main = time.time()
    start_time_tmp = time.time()
    elapsed_time_tmp = 0
    
    if config.ingestion[f'start_ingestion_{coll_name}'] == False:
        print(f'Skip ingestion collection {coll_name}')
        return
    
    if config.ingestion[f'drop_coll_{coll_name}']:
        db = client['db']
        coll = db[coll_name]
        coll.drop()
        if coll_name == 'prices':
            create_timeseries_collection(db, coll_name)
        
    tmp_counter = 0
    counter = 0
    print(f'Ingestion progress collection {coll_name}: {counter}/{len(files)}')
    for f in files:
        if coll_name == 'prices':
            dbh.insert_from_csv(client, f, 'db', coll_name,True)
        else:  
            dbh.insert_from_csv(client, f, 'db', coll_name,False)
         
        tmp_counter+=1
        if tmp_counter == config.ingestion['progress_batch_size']:
            counter+=tmp_counter
            tmp_counter = 0
            
            elapsed_time_tmp = time.time()-start_time_tmp
            start_time_tmp =time.time()
            ingestion_log["elapsed_batch_time"].append(elapsed_time_tmp)
            print(f'Ingestion progress collection {coll_name}: {counter}/{len(files)}, elapsed batch time: {elapsed_time_tmp}')
    
    ingestion_log["complete_elapsed_time"] = time.time() -  start_time_main   
    print(f'Ingestion progress collection {coll_name}: {counter+tmp_counter}/{len(files)}, complete elapsed time: {ingestion_log["complete_elapsed_time"]}')  
    print('Inserted all rows from csv!')
    print("config: ")
    print(config.ingestion)
    print("log: ")
    print(ingestion_log)
    
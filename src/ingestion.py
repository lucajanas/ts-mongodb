
from pymongo import MongoClient
import pandas as pd
import json
import os
import config
import time
import db_handling as dbh
import psutil
import platform
# import cpuinfo
import datetime

ingestion_log = {
    'batch_size':config.ingestion['progress_batch_size'],
    'elapsed_batch_time':[],
    'diff_elapsed_batch_time':[],
    'storage_size':[],            # 'storageSize'
    'data_size':[],               # 'dataSize'
    'diff_data_size':[],
    'complete_elapsed_time':0,
    'collection_name':'',
    'start_time':'',
    'end_time':''
}

def log_ingestion(type):
    if type == 'start':
        now = datetime.datetime.now()
        ingestion_log['start_time'] = now.strftime("%Y-%m-%d %H:%M:%S")
        tmp = f'\n\n{"="*40} Start ingestion on {ingestion_log["start_time"]} {"="*40}\n\n'
        print(tmp)
    
    elif type == 'end':
        now = datetime.datetime.now()
        ingestion_log['end_time'] = now.strftime("%Y-%m-%d %H:%M:%S")
        tmp = f'\n{"="*40} End ingestion on {ingestion_log["end_time"]} {"="*40}\n\n'
        
        # gest system info
        uname = platform.uname()
        cpufreq = psutil.cpu_freq()
        
        system_info = {
        'system':uname.system,
        'release':uname.release,
        'version':uname.version,
        'machine':uname.machine,
        'processor':uname.processor,
        # 'processor_detail':cpuinfo.get_cpu_info()["brand_raw"], 
        'physical_cores':psutil.cpu_count(logical=False),
        'max_frequency':f'{cpufreq.max:.2f}Mhz'
        }

        f_ing_log = open('./log/ingestion_log.json','r')
        # returns JSON object as a dictionary
        log_dic = json.load(f_ing_log)
        f_ing_log.close()
        
        # write all logs
        log_dic[ingestion_log['start_time']]={
            'system_info':system_info,
            'configuration':config.ingestion,
            'log':ingestion_log
        }
        
        # Opening JSON file for write
        f_ing_log = open('./log/ingestion_log.json','w')
        json.dump(log_dic , f_ing_log)
        f_ing_log.close()
        print(tmp)
         
      

def create_timeseries_collection(db, coll_name, time_field='date',meta_field='station_uuid'):
    db.command('create', coll_name, 
               timeseries={ 'timeField': time_field, 'metaField': meta_field })
    

def check_ingestion_config(file_name,coll_name) -> bool:
    """ Files must have this format: 2014-06-08-prices.csv """
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
    
    log_ingestion('start')
    start_time_main = time.time()
    start_time_tmp = time.time()
    elapsed_time_tmp = 0
    
    if config.ingestion[f'start_ingestion_{coll_name}'] == False:
        print(f'Skip ingestion collection {coll_name}')
        return
    else:
        ingestion_log['collection_name'] = coll_name
    
    if config.ingestion[f'drop_coll_{coll_name}']:
        db = client['db']
        coll = db[coll_name]
        coll.drop()
        if coll_name == 'prices':
            create_timeseries_collection(db, coll_name)

    tmp_counter = 0
    counter = 0
    i = 0
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

            # get db stats
            db_stats = db.command({
                'dbStats': 1,
                'scale':  config.ingestion['storage_scale']
            })
            # save in ingestion log
            ingestion_log['storage_size'].append(round(db_stats['storageSize'],2))
            ingestion_log['data_size'].append(round(db_stats['dataSize'],2))
            
            elapsed_time_tmp = round(time.time()-start_time_tmp,2)
            start_time_tmp =time.time()
            ingestion_log['elapsed_batch_time'].append(elapsed_time_tmp)
            
            if i == 0:
                ingestion_log['diff_elapsed_batch_time'].append(0)
                ingestion_log['diff_data_size'].append(0)
            else:
                ingestion_log['diff_elapsed_batch_time'].append(round(ingestion_log['elapsed_batch_time'][i] - ingestion_log['elapsed_batch_time'][i-1],2))
                ingestion_log['diff_data_size'].append(round(ingestion_log['data_size'][i] - ingestion_log['data_size'][i-1],2))
                
            print(f'Ingestion progress collection {coll_name}: {counter}/{len(files)}, elapsed batch time: {elapsed_time_tmp}')
            i+=1
            
    ingestion_log['complete_elapsed_time'] = round(time.time() -  start_time_main,2)  
    ingestion_log['complete_elapsed_time'] = f'complete_elapsed_time: {ingestion_log["complete_elapsed_time"]} sec. / {str(datetime.timedelta(seconds=ingestion_log["complete_elapsed_time"]))}\n'
    print(f'Ingestion progress collection {coll_name}: {counter+tmp_counter}/{len(files)}, complete elapsed time: {ingestion_log["complete_elapsed_time"]}')  
    print('Inserted all rows from csv!')
    log_ingestion('end')

    

from asyncio.windows_events import NULL
import config
import datetime
import json
import time

query_log = {
    'start_timestamp':0,
    'start_time':'',
    'end_time':'',
    'elapsed_time':'',
    'number_of_price_changes':0,
    'list_all_stations_with_changes':[]
}

def log_query(type, parsed_list=NULL):
    
    if type == 'start':
        now = datetime.datetime.now()
        query_log['start_time'] = now.strftime("%Y-%m-%d %H:%M:%S")
        tmp = f'\n\n{"="*40} Start query on {query_log["start_time"]} {"="*40}\n\n'
        query_log['start_timestamp'] = time.time()
        print(tmp)
        
    elif type == 'end':
        
        now = datetime.datetime.now()
        query_log['end_time'] = now.strftime("%Y-%m-%d %H:%M:%S")
        tmp = f'\n{"="*40} End query on {query_log["end_time"]} {"="*40}\n\n'
        
        query_log['elapsed_time'] = round(time.time() - query_log['start_timestamp'] ,2)
        query_log['number_of_price_changes'] = len(parsed_list)
        
        
        if config.query['save_all_stations_with_changes'] == True:
            for x in parsed_list:
                query_log ['list_all_stations_with_changes'].append(x)
        
        
        f_query_log = open('./log/query_log.json','r')
        # returns JSON object as a dictionary
        log_dic = json.load(f_query_log)
        f_query_log.close()
        
        # write all logs
        log_dic[query_log['start_time']]={
     
            'configuration':config.query,
            'log':query_log
        }
        
        # Opening JSON file for write
        f_query_log = open('./log/query_log.json','w')
        json.dump(log_dic , f_query_log)
        f_query_log.close()
        print(tmp)




def query_handling(client):
    
    if config.control['start_query'] == False:
        print("Skip query")
        return
    
    log_query('start')
    mydb = client['db']
    mycol_stations = mydb['stations']
    
    # query stations  (Meschede: 59872)
    # uuid, name, brand, street, house_number, post_code, city,latitude, longitude
    myquery = { 'post_code': config.query['post_code'] }
    station_doc = mycol_stations.find(myquery)
    stations ={}
    list_stations = []
    for x in station_doc:
        #print(f'{x["street"]}  {x["house_number"]}')
        stations[x['uuid']] = x
        list_stations.append(x['uuid'])

    # query date range
    col_prices = mydb['prices']
    list_uuid = []
    start = datetime.datetime(config.query['year_start'],
                              config.query['month_start'],
                              config.query['day_start'],
                              config.query['hour_start'],
                              config.query['minute_start'],
                              config.query['second_start'])
    
    end = datetime.datetime(config.query['year_end'],
                              config.query['month_end'],
                              config.query['day_end'],
                              config.query['hour_end'],
                              config.query['minute_end'],
                              config.query['second_end'])
    
    query = {'date': {'$gte': start, '$lte': end}}
   
    # Specify here which variables are to be saved
    # date, station_uuid, diesel, e5, e10, dieselchange, e5change, e10change  (0=keine Änderung, 1=Änderung, 2=Entfernt, 3=Neu)
    mask = {'date':1, 'station_uuid':1, 'dieselchange':1, 'e5change':1, 'e10change':1}
    prices = col_prices.find(query,mask)
    parsed_list = []
    for x in prices:
        if x['station_uuid'] in list_stations:
            flag_append = False
            if (config.query['change_type'] == "all") and (x['dieselchange'] == 1 or x['e5change'] == 1 or x['e10change'] == 1):
                flag_append = True
            elif config.query['change_type'] == 'dieselchange' and x['dieselchange'] == 1:
                flag_append = True
            elif config.query['change_type'] == 'e5change' and x['e5change'] == 1:
                flag_append = True
            elif config.query['change_type'] == 'e10change' and x['e10change'] == 1:
                flag_append = True
                
            if flag_append == True:
                parsed_list.append({'date': x['date'].strftime("%Y-%m-%d %H:%M:%S"), 
                                    'uuid':x['station_uuid'],
                                    'dieselchange':x['dieselchange'],
                                    'e5change':x['e5change'],
                                    'e10change':x['e10change'],
                                    'street':  stations[x['station_uuid']]['street'],
                                    'house_number':stations[x['station_uuid']]['house_number']})
    
    log_query('end',parsed_list)            
    if config.query['print_interested_stations'] == True:           
        for x in parsed_list:
            print(x)
            print('')
           
    print(f'number of price changes: {len(parsed_list)}')  

    
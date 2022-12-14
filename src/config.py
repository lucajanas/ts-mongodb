import datetime
control = {                                     # Please execute only one of the following 4 possible actions at the same time
    "start_ingestion_prices":False,             # Enable or disable ingestion for collection prices
    "start_ingestion_stations":False,           # Enable or disable ingestion for collection stations
    "start_query":True,                         # Enable or disable query
    "start_resampling":False                    # Enable or disable resampling
}

ingestion = {
    "drop_coll_prices":False,                       # drop(delete) complete price collection if started new ingestion
    "drop_coll_stations":False,                     # drop(delete)  complete price collection if started new ingestion
    "type_prices":"month",                          # available: "month","year","all"
    "year_prices": "2015",                          # available: "2014","2015", ... , "2022"
    "month_prices":"06",                            # available: "01","02","03", ... , "12" (only if stations_type=month)
    "type_stations":"specific",                     # available: "month","year","all","specific"
    "year_stations": "2019",                        # available: "2019","2020",2021,"2022"
    "month_stations":"06",                          # available: "01","02","03", ... , "12" (only if stations_type=month)
    "specific_station":"2022-10-19-stations.csv",   # only this specific csv file
    "progress_batch_size": 20,                      # batch size for comment ingestion progress
    "storage_scale":1000 * 1000,                    # Scale in MB (alternatively 1024 * 1024)
    "desc_scale":"MB"                               # Scale description 
}

query = { 
   "year_start":2022,
   "month_start":9,
   "day_start":28,
   "hour_start":0,
   "minute_start":0,
   "second_start":0,
   "year_end":2022,
   "month_end":9,
   "day_end":30,
   "hour_end":23,
   "minute_end":59,
   "second_end":59,    
   "post_code":"59872",
   "change_type":"all",                           # available: "dieselchange", "e5change", "e10change", "all"
   "print_interested_stations":False,             # if True, then all founded stations are output to the console
   'save_all_stations_with_changes':False         # if True, all founded stations will be stored in query_log.json
}

resampling = {
    'start_year':2015,
    'start_month':6,
    'start_day':4,
    'start_hour':0,
    'start_minute':0,
    'start_second':0,
    'end_year':2015,
    'end_month':6,
    'end_day':30,
    'end_hour':23,
    'end_minute':59,
    'end_second':59,
    'station':'3907c523-3831-4eaf-aec3-d88cc68f5b2e', # station to be resampled
    'frequency':'hour'                                # resampling frequency (available: 'second', 'minute', 'hour', 'day', 'week')
}
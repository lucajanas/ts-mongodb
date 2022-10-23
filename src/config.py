
control = { 
    "start_ingestion_prices":False,             # Enable or disable ingestion for collection prices
    "start_ingestion_stations":False,           # Enable or disable ingestion for collection stations
    "start_query":True,                         # Enable or disable query
}

ingestion = {
    "drop_coll_prices":False,                       # drop(delete) complete price collection if started new ingestion
    "drop_coll_stations":True,                      # drop(delete)  complete price collection if started new ingestion
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
   "year_start":2015,
   "month_start":1,
   "day_start":1,
   "hour_start":0,
   "minute_start":0,
   "second_start":0,
   "year_end":2015,
   "month_end":1,
   "day_end":1,
   "hour_end":23,
   "minute_end":59,
   "second_end":59,    
   "post_code":"32049",
   "change_type":"e5change",                      # available: "dieselchange", "e5change", "e10change", "all"
   "print_interested_stations":True,
   'save_all_stations_with_changes':True
}

ingestion = {
    "start_ingestion_prices":True,     # Enable or disable ingestion for collection prices
    "start_ingestion_stations":False,  # Enable or disable ingestion for collection stations
    "drop_coll_prices":True,           # drop(delete) complete price collection if started new ingestion
    "drop_coll_stations":True,         # drop(delete)  complete price collection if started new ingestion
    "type_prices":"month",              # available: "month","year","all"
    "year_prices": "2015",             # available: "2014","2015", ... , "2022"
    "month_prices":"06",               # available: "01","02","03", ... , "12" (only if stations_type=month)
    "type_stations":"month",           # available: "month","year","all"
    "year_stations": "2019",           # available: "2019","2020",2021,"2022"
    "month_stations":"06",             # available: "01","02","03", ... , "12" (only if stations_type=month)
    "progress_batch_size": 10,         # batch size for comment ingestion progress
    "storage_scale":1000 * 1000,       # Scale in MB (alternatively 1024 * 1024)
    "desc_scale":"MB"                  # Scale description 
}
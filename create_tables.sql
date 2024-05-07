CREATE TABLE `ds525-capstone.ds525_capstone_db.aqi_data` (
    station_id STRING,
    date DATE,
    time TIME,
    date_time DATETIME,
    aqi_val FLOAT64,
    aqi_color_id INT64,
    co_val FLOAT64,
    co_color_id INT64,
    no2_val FLOAT64,
    no2_color_id INT64,
    o3_val FLOAT64,
    o3_color_id INT64,
    pm10_val FLOAT64,
    pm10_color_id INT64,
    pm25_val FLOAT64,
    pm25_color_id INT64,
    so2_val FLOAT64,
    so2_color_id INT64
)
PARTITION BY date;


select 

station.station_id,
station.name_th,
station.name_eng,
station.area_th,
station.area_eng,
station.station_type,
station.location,

val.date, 
val.time, 
cast(val.date_time as DATETIME) as date_time,
val.aqi_val, 
val.aqi_color_id, 
val.co_val, 
val.co_color_id, 
val.no2_val, 
val.no2_color_id, 
val.o3_val, 
val.o3_color_id, 
val.pm10_val, 
val.pm10_color_id,
val.pm25_val, 
val.pm25_color_id, 
val.so2_val, 
val.so2_color_id

from `ds525-capstone.ds525_capstone_db.aqi_data` as val 
join ds525-capstone.ds525_capstone_db.station_data as station
on val.station_id = station.station_id;


select 

station.station_id,
station.name_th,
station.name_eng,
station.area_th,
station.area_eng,
station.station_type,
station.location,

val.date, 
val.time, 
cast(val.date_time as DATETIME) as date_time,
val.aqi_val, 
val.aqi_color_id, 
val.co_val, 
val.co_color_id, 
val.no2_val, 
val.no2_color_id, 
val.o3_val, 
val.o3_color_id, 
val.pm10_val, 
val.pm10_color_id,
val.pm25_val, 
val.pm25_color_id, 
val.so2_val, 
val.so2_color_id

from `ds525-capstone.ds525_capstone_db.aqi_last_data` as val 
join ds525-capstone.ds525_capstone_db.station_data as station
on val.station_id = station.station_id;

select 

station.station_id,
station.name_th,
station.name_eng,
station.area_th,
station.area_eng,
station.station_type,
station.location,

val.date, 
val.time, 
cast(val.date_time as DATETIME) as date_time,
val.aqi_val, 
val.aqi_color_id, 
val.co_val, 
val.co_color_id, 
val.no2_val, 
val.no2_color_id, 
val.o3_val, 
val.o3_color_id, 
val.pm10_val, 
val.pm10_color_id,
val.pm25_val, 
val.pm25_color_id, 
val.so2_val, 
val.so2_color_id

from `ds525-capstone.ds525_capstone_db.aqi_last_data` as val 
join ds525-capstone.ds525_capstone_db.station_data as station
on val.station_id = station.station_id

where area_eng like '%Bangkok%';
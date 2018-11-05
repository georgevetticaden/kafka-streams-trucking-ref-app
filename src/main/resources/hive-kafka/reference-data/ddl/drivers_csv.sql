CREATE EXTERNAL TABLE drivers_csv 
(driverId int, driverName string, certified char(1), wage_plan string)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
WITH SERDEPROPERTIES (
   "separatorChar" = "\t",
   "quoteChar"     = "'",
   "escapeChar"    = "\\"
) 
STORED AS TEXTFILE
LOCATION '/apps/truck_fleet/truck_geo_events';



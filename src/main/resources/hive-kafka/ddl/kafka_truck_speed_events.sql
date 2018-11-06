CREATE EXTERNAL TABLE kafka_truck_speed_events
(eventTime timestamp, eventSource string, truckId int, 
driverId int, driverName string, routeId int, route string, 
speed int 
)

STORED BY 'org.apache.hadoop.hive.kafka.KafkaStorageHandler'

TBLPROPERTIES (
"kafka.topic" = "syndicate-speed-event-json",

"kafka.bootstrap.servers"="g-dps-connected-dp11.field.hortonworks.com:6667,g-dps-connected-dp12.field.hortonworks.com:6667,g-dps-connected-dp13.field.hortonworks.com:6667"
);


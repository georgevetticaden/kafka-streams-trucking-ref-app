CREATE EXTERNAL TABLE kafka_truck_geo_events
(eventTime timestamp, eventSource string, truckId int, 
driverId int, driverName string, 
routeId int, route string, eventType string, 
latitude double, longitude double, correlationId int, 
geoAddress string
)

STORED BY 'org.apache.hadoop.hive.kafka.KafkaStorageHandler'

TBLPROPERTIES(
"kafka.topic" = "syndicate-geo-event-json",
"kafka.sasl.mechanism" = "PLAIN",
"kafka.security.protocol" = "SASL_SSL",
"kafka.sasl.jaas.config" = "org.apache.kafka.common.security.plain.PlainLoginModule required username='srv_truck_data_flow_service' password='XXXXXXXXXXX'",
"kafka.ssl.truststore.location" = "/var/lib/cloudera-scm-agent/agent-cert/cm-auto-global_truststore.jks",
"kafka.bootstrap.servers" = "kafka-b-compute1.field.hortonworks.com:9093,kafka-b-compute2.field.hortonworks.com:9093,kafka-b-compute3.field.hortonworks.com:9093"
);
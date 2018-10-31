CREATE EXTERNAL TABLE druid_kafka_driver_violation_events
(`__time` timestamp, eventSource string, truckId int, driverId int, driverName string, routeId int, route string, eventType string, latitude double, longitude double, correlationId int, geoAddress string)
STORED BY 'org.apache.hadoop.hive.druid.DruidStorageHandler'
TBLPROPERTIES
(
"kafka.bootstrap.servers" = "g-dps-connected-dp11.field.hortonworks.com:6667,g-dps-connected-dp12.field.hortonworks.com:6667,g-dps-connected-dp13.field.hortonworks.com:6667",
"kafka.topic" = "driver-violation-events",
"druid.kafka.ingestion.taskCount" = "1",
"druid.kafka.ingestion.replicas" = "1",
"druid.kafka.ingestion.taskDuration" = "PT1H",
"druid.kafka.ingestion.consumer.security.protocol" = "SASL_PLAINTEXT",
"druid.kafka.ingestion.maxRowsPerSegment" = "5000000",
"druid.segment.granularity" = "HOUR",
"druid.query.granularity" = "HOUR"
);
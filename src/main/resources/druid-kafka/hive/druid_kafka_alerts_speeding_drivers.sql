CREATE EXTERNAL TABLE druid_kafka_alerts_speeding_drivers
(`__time` timestamp, driverId int, driverName string, route string, speed_avg double)
STORED BY 'org.apache.hadoop.hive.druid.DruidStorageHandler'
TBLPROPERTIES
(
"kafka.bootstrap.servers" = "g-dps-connected-dp11.field.hortonworks.com:6667,g-dps-connected-dp12.field.hortonworks.com:6667,g-dps-connected-dp13.field.hortonworks.com:6667",
"kafka.topic" = "alerts-speeding-drivers",
"druid.kafka.ingestion.taskCount" = "1",
"druid.kafka.ingestion.replicas" = "1",
"druid.kafka.ingestion.taskDuration" = "PT1H",
"druid.kafka.ingestion.consumer.security.protocol" = "SASL_PLAINTEXT",
"druid.kafka.ingestion.maxRowsPerSegment" = "5000000",
"druid.segment.granularity" = "HOUR",
"druid.query.granularity" = "HOUR"
);

ALTER TABLE druid_kafka_alerts_speeding_drivers SET TBLPROPERTIES('druid.kafka.ingestion' = 'START');
ALTER TABLE druid_kafka_alerts_speeding_drivers SET TBLPROPERTIES('druid.kafka.ingestion' = 'STOP');
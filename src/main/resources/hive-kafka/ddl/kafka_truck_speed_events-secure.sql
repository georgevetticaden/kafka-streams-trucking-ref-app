CREATE EXTERNAL TABLE kafka_truck_speed_events
(eventTime timestamp, eventSource string, truckId int, 
driverId int, driverName string, routeId int, route string, 
speed int 
)

STORED BY 'org.apache.hadoop.hive.kafka.KafkaStorageHandler'

TBLPROPERTIES (
"kafka.topic" = "syndicate-speed-event-json",
"kafka.consumer.sasl.mechanism" = "GSSAPI",
"kafka.consumer.sasl.kerberos.service.name"="kafka",
"kafka.consumer.security.protocol" = "SASL_SSL",
"kafka.consumer.ssl.truststore.location" = "/var/lib/cloudera-scm-agent/agent-cert/cm-auto-global_truststore.jks",
"kafka.bootstrap.servers" = "kafka-b-compute1.field.hortonworks.com:9093,kafka-b-compute2.field.hortonworks.com:9093,kafka-b-compute3.field.hortonworks.com:9093",
"kafka.consumer.sasl.jaas.config"="com.sun.security.auth.module.Krb5LoginModule required useKeyTab=true storeKey=true keyTab='/tmp/truck_hive_kafka_service.keytab' principal='truck_hive_kafka_service@STREAMANALYTICS'\u003B"
);
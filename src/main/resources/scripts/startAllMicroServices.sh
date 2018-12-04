#!/bin/bash

#export JAVA_HOME=$(find /usr/jdk64 -iname 'jdk1.8*' -type d)
#export PATH=$PATH:$JAVA_HOME/bin
export MICRO_SERVICES_JAR=/Users/gvetticaden/Dropbox/Hortonworks/Development/Git/kafka-streams-trucking-ref-app/target/kafka-streams-microservices-jar-with-dependencies.jar


export kafkaBrokers="j-dps-connected-dp11.field.hortonworks.com:6667,j-dps-connected-dp12.field.hortonworks.com:6667,j-dps-connected-dp13.field.hortonworks.com:6667"
export schemaRegistryUrl=http://j-dps-connected-dp3.field.hortonworks.com:7788/api/v1

export securityProtocol=SASL_PLAINTEXT



startJoinFilterMicroService1() {
         java -Djava.security.auth.login.config=/Users/gvetticaden/Dropbox/Hortonworks/Development/Git/kafka-streams-trucking-ref-app/src/main/resources/jaas/micro-service-join-filter_jaas.conf \
         		-cp $MICRO_SERVICES_JAR \
                hortonworks.hdf.kafkastreams.refapp.truck.microservice.JoinFilterGeoSpeedMicroService \
                --bootstrap.servers $kafkaBrokers \
                --schema.registry.url $schemaRegistryUrl \
                --groupId truckGeoSpeedJoinMicroService \
                --clientId join-filter-geo-speed-ms-1 \
                --security.protocol $securityProtocol \
                --auto.offset.reset latest >  "join-filter-micro-service.log" &
}

startCalculateDriverAvgSpeedMicroService() {
         java -Djava.security.auth.login.config=/Users/gvetticaden/Dropbox/Hortonworks/Development/Git/kafka-streams-trucking-ref-app/src/main/resources/jaas/micro-service-calculate-driver-avg_jaas.conf \
         		-cp $MICRO_SERVICES_JAR \
                hortonworks.hdf.kafkastreams.refapp.truck.microservice.CalculateDriverAvgSpeedMicroService \
                --bootstrap.servers $kafkaBrokers \
                --schema.registry.url $schemaRegistryUrl \
                --groupId calculateDriverAvgSpeedMicroService \
                --clientId calculate-driver-avg-speed-ms-1 \
                --security.protocol $securityProtocol \
                --auto.offset.reset latest >  "calculate-driver-avg-speed-micro-service.log" &
}

startAlertSpeedingDriversMicroService() {
         java -Djava.security.auth.login.config=/Users/gvetticaden/Dropbox/Hortonworks/Development/Git/kafka-streams-trucking-ref-app/src/main/resources/jaas/micro-service-alert-speeding-drivers_jaas.conf \
         		-cp $MICRO_SERVICES_JAR \
                hortonworks.hdf.kafkastreams.refapp.truck.microservice.AlertSpeedingDriversMicroService \
                --bootstrap.servers $kafkaBrokers \
                --schema.registry.url $schemaRegistryUrl \
                --groupId alertSpeedingDriversMicroService \
                --clientId alert-speeding-driver-ms-1 \
                --security.protocol $securityProtocol \
                --auto.offset.reset latest >  "alert-speeding-drivers-micro-service.log" &
}

startJoinFilterMicroService1
startCalculateDriverAvgSpeedMicroService
startAlertSpeedingDriversMicroService
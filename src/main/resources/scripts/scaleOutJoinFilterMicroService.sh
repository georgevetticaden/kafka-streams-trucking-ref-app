#!/bin/bash

#export JAVA_HOME=$(find /usr/jdk64 -iname 'jdk1.8*' -type d)
#export PATH=$PATH:$JAVA_HOME/bin
export MICRO_SERVICES_JAR=/Users/gvetticaden/Dropbox/Hortonworks/Development/Git/kafka-streams-trucking-ref-app/target/kafka-streams-microservices-jar-with-dependencies.jar


export kafkaBrokers="j-dps-connected-dp11.field.hortonworks.com:6667,j-dps-connected-dp12.field.hortonworks.com:6667,j-dps-connected-dp13.field.hortonworks.com:6667"
export schemaRegistryUrl=http://j-dps-connected-dp3.field.hortonworks.com:7788/api/v1

export securityProtocol=SASL_PLAINTEXT



killMicroService1() {
	kill $(ps aux | grep 'JoinFilterGeoSpeedMicroService' | awk '{print $2}')
}

startJoinFilterMicroService2() {
		echo "Scaling out JoinFilterMicroService by adding second consumer with clientId[join-filter-geo-speed-ms-2] in existing consumer-group[truck-micro-service-geo-speed-join-filter]"
		
         java -Djava.security.auth.login.config=/Users/gvetticaden/Dropbox/Hortonworks/Development/Git/kafka-streams-trucking-ref-app/src/main/resources/jaas/micro-service-join-filter_jaas.conf \
         		-cp $MICRO_SERVICES_JAR \
                hortonworks.hdf.kafkastreams.refapp.truck.microservice.JoinFilterGeoSpeedMicroService \
                --bootstrap.servers $kafkaBrokers \
                --schema.registry.url $schemaRegistryUrl \
                --groupId truck-micro-service-geo-speed-join-filter \
                --clientId join-filter-geo-speed-ms-2 \
                --security.protocol $securityProtocol \
                --auto.offset.reset latest >  "join-filter-micro-service-2.log" &
}

startJoinFilterMicroService3() {
		echo "Scaling out JoinFilterMicroService by adding third consumer with clientId[join-filter-geo-speed-ms-3] in existing consumer-group[truck-micro-service-geo-speed-join-filter]"
		
         java -Djava.security.auth.login.config=/Users/gvetticaden/Dropbox/Hortonworks/Development/Git/kafka-streams-trucking-ref-app/src/main/resources/jaas/micro-service-join-filter_jaas.conf \
         		-cp $MICRO_SERVICES_JAR \
                hortonworks.hdf.kafkastreams.refapp.truck.microservice.JoinFilterGeoSpeedMicroService \
                --bootstrap.servers $kafkaBrokers \
                --schema.registry.url $schemaRegistryUrl \
                --groupId truck-micro-service-geo-speed-join-filter \
                --clientId join-filter-geo-speed-ms-3 \
                --security.protocol $securityProtocol \
                --auto.offset.reset latest >  "join-filter-micro-service-3.log" &
}

startJoinFilterMicroService1() {
         java -Djava.security.auth.login.config=/Users/gvetticaden/Dropbox/Hortonworks/Development/Git/kafka-streams-trucking-ref-app/src/main/resources/jaas/micro-service-join-filter_jaas.conf \
         		-cp $MICRO_SERVICES_JAR \
                hortonworks.hdf.kafkastreams.refapp.truck.microservice.JoinFilterGeoSpeedMicroService \
                --bootstrap.servers $kafkaBrokers \
                --schema.registry.url $schemaRegistryUrl \
                --groupId truck-micro-service-geo-speed-join-filter \
                --clientId join-filter-geo-speed-ms-1 \
                --security.protocol $securityProtocol \
                --auto.offset.reset latest >  "join-filter-micro-service.log" &
}

killMicroService1
startJoinFilterMicroService1
startJoinFilterMicroService2
startJoinFilterMicroService3

package hortonworks.hdf.kafkastreams.refapp.truck.consumer;

import hortonworks.hdf.kafkastreams.refapp.BaseStreamsApp;
import hortonworks.hdf.schema.refapp.trucking.TruckGeoEventEnriched;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;



public class GeoStreamConsumer extends BaseStreamsApp {
	
	
	private static final Logger LOGGER = LoggerFactory.getLogger(GeoStreamConsumer.class); 	
	private static final String GEO_STREAM_TOPIC = "syndicate-geo-event-avro";
	
	public GeoStreamConsumer(Properties configs) {
		super(configs);
	}

	public static void main(String[] args) {
		
		Properties kafkaConfig = createKafkaConfiguration(args);
		GeoStreamConsumer geoStreamConsumer = new GeoStreamConsumer(kafkaConfig);
		geoStreamConsumer.consume();
		
	}
	
	
	public void consume() {
		try (KafkaConsumer<Integer, TruckGeoEventEnriched> consumer = new KafkaConsumer<>(configs)) {
            consumer.subscribe(Collections.singleton(GEO_STREAM_TOPIC));
            while (true) {
                final ConsumerRecords<Integer, TruckGeoEventEnriched> consumerRecords = consumer.poll(Duration.ofSeconds(1));
                LOGGER.info("Number of Records consumed is: " + consumerRecords.count());
                
                for(ConsumerRecord<Integer, TruckGeoEventEnriched> truckGeoEventRecord: consumerRecords) {
                	TruckGeoEventEnriched truckGeoEvent = truckGeoEventRecord.value();
                	LOGGER.info("Key["+truckGeoEventRecord.key()+"], the The geo event for Driver["+truckGeoEvent.getDriverName() +"] is: " + truckGeoEvent.toString());
                }
            }
        }		
	}


	

}

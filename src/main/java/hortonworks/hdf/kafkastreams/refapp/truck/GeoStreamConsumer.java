package hortonworks.hdf.kafkastreams.refapp.truck;

import hortonworks.hdf.kafkastreams.refapp.wordcount.WordCount;
import hortonworks.hdf.schema.refapp.trucking.TruckGeoEventEnriched;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;



public class GeoStreamConsumer extends AbstractConsumeLoop {
	
	
	private static final Logger LOGGER = LoggerFactory.getLogger(WordCount.class); 	
	private static final String GEO_STREAM_TOPIC = "syndicate-geo-event-avro";
	
	private Properties configs;

	public GeoStreamConsumer(Properties configs) {
		this.configs = configs;
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
                	LOGGER.info(truckGeoEvent.toString());
                }
            }
        }		
	}


	

}

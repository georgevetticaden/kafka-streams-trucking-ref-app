package hortonworks.hdf.kafkastreams.refapp.truck.consumer;

import hortonworks.hdf.kafkastreams.refapp.BaseStreamsApp;
import hortonworks.hdf.schema.refapp.trucking.TruckSpeedEventEnriched;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;



public class SpeedStreamConsumer extends BaseStreamsApp {
	
	
	private static final Logger LOGGER = LoggerFactory.getLogger(SpeedStreamConsumer.class); 	
	private static final String SPEED_STREAM_TOPIC = "syndicate-speed-event-avro";
	

	public SpeedStreamConsumer(Properties configs) {
		super(configs);
	}

	public static void main(String[] args) {
		
		Properties kafkaConfig = createKafkaConfiguration(args);
		SpeedStreamConsumer speedStreamConsumer = new SpeedStreamConsumer(kafkaConfig);
		speedStreamConsumer.consume();
		
	}
	
	
	public void consume() {
		try (KafkaConsumer<Integer, TruckSpeedEventEnriched> consumer = new KafkaConsumer<>(configs)) {
            consumer.subscribe(Collections.singleton(SPEED_STREAM_TOPIC));
            while (true) {
                final ConsumerRecords<Integer, TruckSpeedEventEnriched> consumerRecords = consumer.poll(Duration.ofSeconds(1));
                LOGGER.info("Number of Records consumed is: " + consumerRecords.count());
                
                for(ConsumerRecord<Integer, TruckSpeedEventEnriched> truckSpeedEventRecord: consumerRecords) {
                	TruckSpeedEventEnriched truckSpeedEvent = truckSpeedEventRecord.value();
                	LOGGER.info("Key["+truckSpeedEventRecord.key()+"],The speed event for Driver["+truckSpeedEvent.getDriverName() +"] is: " + truckSpeedEvent.toString());
                }
            }
        }		
	}


	

}

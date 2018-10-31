package hortonworks.hdf.kafkastreams.refapp.truck.consumer;

import hortonworks.hdf.kafkastreams.refapp.BaseStreamsApp;
import hortonworks.hdf.schema.refapp.trucking.TruckSpeedEventEnriched;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.hortonworks.registries.schemaregistry.serdes.avro.kafka.KafkaAvroDeserializer;



public class DriverViolationEventsConsumer extends BaseStreamsApp {
	
	
	private static final Logger LOGGER = LoggerFactory.getLogger(DriverViolationEventsConsumer.class); 	
	private static final String DRIVER_VIOLATION_EVENTS_TOPIC= "driver-violation-events";
	

	public DriverViolationEventsConsumer(Properties configs) {
		super(configs);
		overrideSerdes(configs);
	}


	public static void main(String[] args) {
		
		Properties kafkaConfig = createKafkaConfiguration(args);
		DriverViolationEventsConsumer speedStreamConsumer = new DriverViolationEventsConsumer(kafkaConfig);
		speedStreamConsumer.consume();
		
	}
	
	
	public void consume() {
		try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(configs)) {
            consumer.subscribe(Collections.singleton(DRIVER_VIOLATION_EVENTS_TOPIC));
            while (true) {
                final ConsumerRecords<String, String> consumerRecords = consumer.poll(Duration.ofSeconds(1));
                //LOGGER.info("Number of Records consumed is: " + consumerRecords.count());
                
                for(ConsumerRecord<String, String> record: consumerRecords) {
                	String recordValue = record.value();
                	LOGGER.info("Key["+record.key()+"],temp value is: " + recordValue);
                }
            }
        }		
	}


	private void overrideSerdes(Properties configs) {
		configs.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
		configs.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);  
		
	}
	

}

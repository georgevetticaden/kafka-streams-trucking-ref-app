package hortonworks.hdf.kafkastreams.refapp.truck.consumer;

import hortonworks.hdf.kafkastreams.refapp.BaseConsumerClient;

import java.time.Duration;
import java.util.Collections;
import java.util.Map;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;



public class DriverAverageSpeedConsumer extends BaseConsumerClient {
	
	
	private static final Logger LOGGER = LoggerFactory.getLogger(DriverAverageSpeedConsumer.class); 	;
	private static final String DRIVER_AVG_SPEED_TOPIC= "driver-average-speed";
	

	public DriverAverageSpeedConsumer(Map<String, Object> configs) {
		super(configs);

	}


	public static void main(String[] args) {
		
		Map<String, Object> consumerConfig = createKafkaConfiguration(args);
		DriverAverageSpeedConsumer speedStreamConsumer = new DriverAverageSpeedConsumer(consumerConfig);
		speedStreamConsumer.consume();
		
	}
	
	
	public void consume() {
		try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(configs)) {
            consumer.subscribe(Collections.singleton(DRIVER_AVG_SPEED_TOPIC));
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

}

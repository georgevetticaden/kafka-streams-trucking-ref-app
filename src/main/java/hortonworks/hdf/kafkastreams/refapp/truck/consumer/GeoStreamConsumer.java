package hortonworks.hdf.kafkastreams.refapp.truck.consumer;

import hortonworks.hdf.kafkastreams.refapp.BaseConsumerClient;
import hortonworks.hdf.schema.refapp.trucking.TruckGeoEventEnriched;

import java.time.Duration;
import java.util.Collections;
import java.util.Map;
import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.hortonworks.registries.schemaregistry.client.SchemaRegistryClient;
import com.hortonworks.registries.schemaregistry.serdes.avro.AbstractAvroSnapshotDeserializer;
import com.hortonworks.registries.schemaregistry.serdes.avro.kafka.KafkaAvroDeserializer;



public class GeoStreamConsumer extends BaseConsumerClient {
	
	
	private static final Logger LOGGER = LoggerFactory.getLogger(GeoStreamConsumer.class); 	
	private static final String GEO_STREAM_TOPIC = "syndicate-geo-event-avro";
	
	public GeoStreamConsumer(Map<String, Object> configs) {
		super(configs);
	}

	public static void main(String[] args) {
		
		Map<String, Object> consumerConfig = createKafkaConfiguration(args);
		GeoStreamConsumer geoStreamConsumer = new GeoStreamConsumer(consumerConfig);
		geoStreamConsumer.consume();
		
	}
	
	@Override
	protected void configureSerdes(Properties props, Map<String, Object> configMap) {
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);     
        
        props.put(SchemaRegistryClient.Configuration.SCHEMA_REGISTRY_URL.name(), configMap.get("schema.registry.url"));
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, KafkaAvroDeserializer.class);        

        /* This is what enables to serilaize from Avro to Pojo object */
        props.put(AbstractAvroSnapshotDeserializer.SPECIFIC_AVRO_READER, true);
	}	
	
	public void consume() {
		try (KafkaConsumer<Integer, TruckGeoEventEnriched> consumer = new KafkaConsumer<>(configs)) {
            consumer.subscribe(Collections.singleton(GEO_STREAM_TOPIC));
            while (true) {
                final ConsumerRecords<Integer, TruckGeoEventEnriched> consumerRecords = consumer.poll(Duration.ofSeconds(1));
                
                for(ConsumerRecord<Integer, TruckGeoEventEnriched> truckGeoEventRecord: consumerRecords) {
                	TruckGeoEventEnriched truckGeoEvent = truckGeoEventRecord.value();
                	LOGGER.info("Key["+truckGeoEventRecord.key()+"], the The geo event for Driver["+truckGeoEvent.getDriverName() +"] is: " + truckGeoEvent.toString());
                }
            }
        }		
	}


	

}

package hortonworks.hdf.kafkastreams.refapp.truck.microservice;

import hortonworks.hdf.kafkastreams.refapp.BaseStreamsApp;
import hortonworks.hdf.kafkastreams.refapp.truck.dto.TruckGeoSpeedJoin;
import hortonworks.hdf.kafkastreams.refapp.truck.serde.TruckGeoSpeedJoinSerde;
import hortonworks.hdf.schema.refapp.trucking.TruckGeoEventEnriched;
import hortonworks.hdf.schema.refapp.trucking.TruckSpeedEventEnriched;

import java.util.Map;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.JoinWindows;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Predicate;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.kstream.ValueJoiner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.hortonworks.registries.schemaregistry.client.SchemaRegistryClient;
import com.hortonworks.registries.schemaregistry.serdes.avro.AbstractAvroSnapshotDeserializer;
import com.hortonworks.registries.schemaregistry.serdes.avro.kafka.KafkaAvroSerde;
import com.hortonworks.registries.schemaregistry.serdes.avro.kafka.KafkaAvroSerializer;

public class JoinFilterGeoSpeedMicroService extends BaseStreamsApp {

	private static final Logger LOGGER = LoggerFactory.getLogger(JoinFilterGeoSpeedMicroService.class);			
	
	private static final String STREAMS_APP_ID = "truck-geo-speed--join-filter-micro-service";
	
	private static final String SOURCE_GEO_STREAM_TOPIC = "syndicate-geo-event-avro";	
	private static final String SOURCE_SPEED_STREAM_TOPIC = "syndicate-speed-event-avro";
	private static final String SINK_DRIVER_VIOLATION_EVENTS_TOPIC= "driver-violation-events";
	

	
	public JoinFilterGeoSpeedMicroService(Map<String, Object> kafkaConfigMap) {
		super(kafkaConfigMap, STREAMS_APP_ID );
		/* Override with the SR Serdes */
		configureSerdes(configs, kafkaConfigMap);
	}

	public static void main(String[] args) {
		
		Map<String, Object> consumerConfig = createKafkaConfiguration(args);
		JoinFilterGeoSpeedMicroService speedingTruckDriversApp = new JoinFilterGeoSpeedMicroService(consumerConfig);
		speedingTruckDriversApp.run();
		
	}	
	
	public void run() {
		
		/* Build teh kafka Streams Topology */
        KafkaStreams truckGeoSpeedJoinMicroService = buildKafkaStreamsApp();
		
        final CountDownLatch latch = new CountDownLatch(1);
		 
		// attach shutdown handler to catch control-c
        Runtime.getRuntime().addShutdownHook(new Thread("streams-shutdown-hook") {
            @Override
            public void run() {
            	truckGeoSpeedJoinMicroService.close();
                latch.countDown();
            }
        });

        try {
        	truckGeoSpeedJoinMicroService.start();
            latch.await();
        } catch (Throwable e) {
            System.exit(1);
        }
        System.exit(0);		
	}

	private KafkaStreams buildKafkaStreamsApp() {
		
		StreamsBuilder builder = new StreamsBuilder();
        

		
        /* Create the 2 Streams */
        final KStream<String, TruckGeoEventEnriched> geoStream = builder.stream(SOURCE_GEO_STREAM_TOPIC);		
        final KStream<String, TruckSpeedEventEnriched> speedStream = builder.stream(SOURCE_SPEED_STREAM_TOPIC);		

        /* Join the Streams */
        final KStream<String, TruckGeoSpeedJoin> joinedStream = joinStreams(geoStream, speedStream);
		
        /* Filter the Stream for violation events */
        final KStream<String, TruckGeoSpeedJoin> filteredStream = filterStreamForViolationEvents(joinedStream);
        
        /* Write the violation events to the violation topic */
        filteredStream.to(SINK_DRIVER_VIOLATION_EVENTS_TOPIC, Produced.with(new Serdes.StringSerde(), new TruckGeoSpeedJoinSerde()));
        
		
		/* Build Topology */
		Topology streamsTopology = builder.build();

		LOGGER.debug("Truck-Join-And-Filter-Micro-Service Topoogy is: " + streamsTopology.describe());
		
		/* Create Streams App */
		KafkaStreams speedingDriversStreamsApps = new KafkaStreams(streamsTopology, configs);
		return speedingDriversStreamsApps;
	}


	private KStream<String, TruckGeoSpeedJoin> filterStreamForViolationEvents(
			final KStream<String, TruckGeoSpeedJoin> joinedStream) {
		Predicate<String, TruckGeoSpeedJoin> violationEventPredicate = new Predicate<String, TruckGeoSpeedJoin>() {

			@Override
			public boolean test(String key, TruckGeoSpeedJoin truckGeo) {
				return !"Normal".equals(truckGeo.getEventType());
			}
		};
		/* Filter for Violation events on the stream */
        final KStream<String, TruckGeoSpeedJoin> filteredStream = joinedStream.filter(violationEventPredicate);
		return filteredStream;
	}



	private KStream<String, TruckGeoSpeedJoin> joinStreams(
			final KStream<String, TruckGeoEventEnriched> geoStream,
			final KStream<String, TruckSpeedEventEnriched> speedStream) {
		
		ValueJoiner<TruckGeoEventEnriched, TruckSpeedEventEnriched, TruckGeoSpeedJoin> joiner = 
        		new ValueJoiner<TruckGeoEventEnriched, TruckSpeedEventEnriched, TruckGeoSpeedJoin>() {

			@Override
			public TruckGeoSpeedJoin apply(TruckGeoEventEnriched geoStreamJoin,
					TruckSpeedEventEnriched speedStreamJoin) {
				return new TruckGeoSpeedJoin(geoStreamJoin, speedStreamJoin);
			}
		};
		
		
		/* Window time of 1.5 seconds */
        long windowTime = 1500;
		JoinWindows joinWindow = JoinWindows.of(windowTime);
		
		/* Join the Two Streams */
		final KStream<String, TruckGeoSpeedJoin> joinedStream = geoStream.join(speedStream, joiner, joinWindow);
		return joinedStream;
	}
		
	@Override
	protected void configureSerdes(Properties props, Map<String, Object> result) {
		props.put(SchemaRegistryClient.Configuration.SCHEMA_REGISTRY_URL.name(), result.get("schema.registry.url"));
        props.put(AbstractAvroSnapshotDeserializer.SPECIFIC_AVRO_READER, true);
        props.put(KafkaAvroSerializer.STORE_SCHEMA_VERSION_ID_IN_HEADER,
                STORE_SCHEMA_VERSION_ID_IN_HEADER_POLICY);    		
        
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.StringSerde.class);
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, KafkaAvroSerde.class);        
  
	}		
	
}

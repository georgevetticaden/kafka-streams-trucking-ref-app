package hortonworks.hdf.kafkastreams.refapp.truck;

import hortonworks.hdf.kafkastreams.refapp.BaseStreamsApp;
import hortonworks.hdf.kafkastreams.refapp.truck.aggregrator.DriverAvgSpeedAgregrator;
import hortonworks.hdf.kafkastreams.refapp.truck.aggregrator.DriverSpeedAvgValue;
import hortonworks.hdf.kafkastreams.refapp.truck.aggregrator.DriverSpeedRunningCountAndSum;
import hortonworks.hdf.kafkastreams.refapp.truck.dto.TruckGeoSpeedJoin;
import hortonworks.hdf.kafkastreams.refapp.truck.serde.DriverSpeedRunningCountAndSumSerde;
import hortonworks.hdf.kafkastreams.refapp.truck.serde.JsonPOJODeserializer;
import hortonworks.hdf.kafkastreams.refapp.truck.serde.JsonPOJOSerializer;
import hortonworks.hdf.kafkastreams.refapp.truck.serde.WindowSerde;
import hortonworks.hdf.schema.refapp.trucking.TruckGeoEventEnriched;
import hortonworks.hdf.schema.refapp.trucking.TruckSpeedEventEnriched;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.Initializer;
import org.apache.kafka.streams.kstream.JoinWindows;
import org.apache.kafka.streams.kstream.KGroupedStream;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.KeyValueMapper;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Predicate;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.kstream.TimeWindowedKStream;
import org.apache.kafka.streams.kstream.TimeWindows;
import org.apache.kafka.streams.kstream.ValueJoiner;
import org.apache.kafka.streams.kstream.ValueMapper;
import org.apache.kafka.streams.kstream.Windowed;
import org.apache.kafka.streams.state.WindowStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SpeedingTruckDriversStreamsApp extends BaseStreamsApp {

	private static final Logger LOGGER = LoggerFactory.getLogger(SpeedingTruckDriversStreamsApp.class);
			
	private static final String GEO_STREAM_TOPIC = "syndicate-geo-event-avro";	
	private static final String SPEED_STREAM_TOPIC = "syndicate-speed-event-avro";
	private static final String TEMP_TRUCK_STREAMS_TOPIC = "temp-truck-streams-output";
	
	
	public SpeedingTruckDriversStreamsApp(Properties kafkaConfig) {
		super(kafkaConfig);
		kafkaConfig.put(StreamsConfig.APPLICATION_ID_CONFIG, "speeding-drivers");
	}

	public static void main(String[] args) {
		
		Properties kafkaConfig = createKafkaConfiguration(args);
		SpeedingTruckDriversStreamsApp speedingTruckDriversApp = new SpeedingTruckDriversStreamsApp(kafkaConfig);
		speedingTruckDriversApp.run();
		
	}	
	
	public void run() {
		
        KafkaStreams speedingDriversStreamsApps = buildKafkaStreamsSpeedingDriversApp();
		
        final CountDownLatch latch = new CountDownLatch(1);
		 
		// attach shutdown handler to catch control-c
        Runtime.getRuntime().addShutdownHook(new Thread("streams-shutdown-hook") {
            @Override
            public void run() {
            	speedingDriversStreamsApps.close();
                latch.countDown();
            }
        });

        try {
        	speedingDriversStreamsApps.start();
            latch.await();
        } catch (Throwable e) {
            System.exit(1);
        }
        System.exit(0);		
	}

	private KafkaStreams buildKafkaStreamsSpeedingDriversApp() {
		
		StreamsBuilder builder = new StreamsBuilder();
        
        /* Create the 2 Streams */
        final KStream<String, TruckGeoEventEnriched> geoStream = builder.stream(GEO_STREAM_TOPIC);		
        final KStream<String, TruckSpeedEventEnriched> speedStream = builder.stream(SPEED_STREAM_TOPIC);		

        /* Join the Streams */
        final KStream<String, TruckGeoSpeedJoin> joinedStream = joinStreams(geoStream, speedStream);
		
        /* Filter the Stream for violation events */
        final KStream<String, TruckGeoSpeedJoin> filteredStream = filterStream(joinedStream);
        
        
        /* Calculate average speed of driver over 3 minute window */
		KTable<Windowed<String>, DriverSpeedAvgValue> driverAvgSpeedTable = calculateDriverAvgSpeedOver3MinuteWindow(filteredStream);
		
		/* Write Stream to Test Topic */
		writeDriverAvgTableToTestTopic(driverAvgSpeedTable);
				
		
		/* Build Topology */
		Topology streamsTopology = builder.build();
		
		
		
		LOGGER.debug("Speeding Driver Streams App Topoogy is: " + streamsTopology.describe());
		
		/* Create Streams App */
		KafkaStreams speedingDriversStreamsApps = new KafkaStreams(streamsTopology, configs);
		return speedingDriversStreamsApps;
	}

	private KTable<Windowed<String>, DriverSpeedAvgValue> calculateDriverAvgSpeedOver3MinuteWindow(
			final KStream<String, TruckGeoSpeedJoin> filteredStream) {
		//first group by driverId
        KGroupedStream<String, TruckGeoSpeedJoin> groupedStream = filteredStream.groupByKey();
        
        //Create a three minute window
        TimeWindows window = TimeWindows.of(TimeUnit.MINUTES.toMillis(3));
		TimeWindowedKStream<String, TruckGeoSpeedJoin> windowStream = groupedStream.windowedBy(window);
		
		//Do Aggregration
		Initializer<DriverSpeedRunningCountAndSum> avgInitializer = new Initializer<DriverSpeedRunningCountAndSum>() {

			@Override
			public DriverSpeedRunningCountAndSum apply() {
				return new DriverSpeedRunningCountAndSum();
			}
		};
		
		
		//Calculate Average on the aggregration table
		
		KTable<Windowed<String>, DriverSpeedRunningCountAndSum> driverSpeedSumAndCountTable = 
				windowStream.aggregate(avgInitializer, 
									   new DriverAvgSpeedAgregrator(),
									   Materialized.<String, DriverSpeedRunningCountAndSum, WindowStore<Bytes, byte[]>>as("time-windowed-aggregated-stream-store")
								          .withValueSerde(new DriverSpeedRunningCountAndSumSerde())
								        );
                
		
		ValueMapper<DriverSpeedRunningCountAndSum, DriverSpeedAvgValue> averageMapper = new ValueMapper<DriverSpeedRunningCountAndSum, DriverSpeedAvgValue>() {

			@Override
			public DriverSpeedAvgValue apply(DriverSpeedRunningCountAndSum driverSpeedSumAndCountValue) {
				double average = driverSpeedSumAndCountValue.getRunningSum() / driverSpeedSumAndCountValue.getRunningCount();
				LOGGER.debug("Completed avarage aggregration for Driver["+ driverSpeedSumAndCountValue.getDriverName()+"], runningCount["+driverSpeedSumAndCountValue.getRunningCount()+"], average speed is: " + average);
				return new DriverSpeedAvgValue(driverSpeedSumAndCountValue.getDriverId(), driverSpeedSumAndCountValue.getDriverName(), average);
			}
		};
		
		
		KTable<Windowed<String>, DriverSpeedAvgValue> driverAvgSpeedTable = driverSpeedSumAndCountTable.mapValues(averageMapper);
		return driverAvgSpeedTable;
	}







	private KStream<String, TruckGeoSpeedJoin> filterStream(
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
	
	private void writeStreamToTestTopic(final KStream<String, TruckGeoSpeedJoin> joinedStream) {
		KeyValueMapper<String, TruckGeoSpeedJoin, Iterable<KeyValue<String, String>>> mapper = 
				new KeyValueMapper<String, TruckGeoSpeedJoin, Iterable<KeyValue<String,String>>>() {

			@Override
			public Iterable<KeyValue<String, String>> apply(String key,
					TruckGeoSpeedJoin geoSpeedJoin) {
				List<KeyValue<String, String>> result = new ArrayList<KeyValue<String,String>>();
				result.add(new KeyValue<String, String>(key, geoSpeedJoin.toString()));
				return result;
			}
		};
		
		/* Write Stream to temp test topic */
        final KStream<String, String> outputForTempTopic = joinedStream.flatMap(mapper);
        outputForTempTopic.to(TEMP_TRUCK_STREAMS_TOPIC, Produced.with(Serdes.String(), Serdes.String()));
	}	

	private void writeDriverAvgTableToTestTopic(
			KTable<Windowed<String>, DriverSpeedAvgValue> driverAvgSpeedTable) {
		
		KStream<Windowed<String>, DriverSpeedAvgValue> outputForTempTopic =  driverAvgSpeedTable.toStream();

		Serde<DriverSpeedAvgValue> driverAvgSpeedSerde = new Serde<DriverSpeedAvgValue>() {

			@Override
			public void configure(Map<String, ?> configs, boolean isKey) {
				// TODO Auto-generated method stub
				
			}

			@Override
			public void close() {
				// TODO Auto-generated method stub
				
			}

			@Override
			public Serializer<DriverSpeedAvgValue> serializer() {
				return new JsonPOJOSerializer<DriverSpeedAvgValue>();
			}

			@Override
			public Deserializer<DriverSpeedAvgValue> deserializer() {
				return new JsonPOJODeserializer<DriverSpeedAvgValue>(DriverSpeedAvgValue.class);
			}
		};
		outputForTempTopic.to(TEMP_TRUCK_STREAMS_TOPIC, Produced.with(new WindowSerde(), driverAvgSpeedSerde ));
		
		
	}	
}

package hortonworks.hdf.kafkastreams.refapp.truck.microservice;

import hortonworks.hdf.kafkastreams.refapp.BaseStreamsApp;
import hortonworks.hdf.kafkastreams.refapp.truck.aggregrator.DriverAvgSpeedAgregrator;
import hortonworks.hdf.kafkastreams.refapp.truck.aggregrator.DriverSpeedAvgValue;
import hortonworks.hdf.kafkastreams.refapp.truck.aggregrator.DriverSpeedRunningCountAndSum;
import hortonworks.hdf.kafkastreams.refapp.truck.dto.TruckGeoSpeedJoin;
import hortonworks.hdf.kafkastreams.refapp.truck.serde.DriverSpeedAvgValueSerde;
import hortonworks.hdf.kafkastreams.refapp.truck.serde.DriverSpeedRunningCountAndSumSerde;
import hortonworks.hdf.kafkastreams.refapp.truck.serde.TruckGeoSpeedJoinSerde;
import hortonworks.hdf.kafkastreams.refapp.truck.serde.WindowSerde;

import java.util.Date;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Initializer;
import org.apache.kafka.streams.kstream.KGroupedStream;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.kstream.TimeWindowedKStream;
import org.apache.kafka.streams.kstream.TimeWindows;
import org.apache.kafka.streams.kstream.ValueMapper;
import org.apache.kafka.streams.kstream.Windowed;
import org.apache.kafka.streams.state.WindowStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CalculateDriverAvgSpeedMicroService extends BaseStreamsApp {

	private static final Logger LOGGER = LoggerFactory.getLogger(CalculateDriverAvgSpeedMicroService.class);			
	
	private static final String STREAMS_APP_ID = "truck-micro-service-truck-calculate-driver-avg";
	
	private static final String SOURCE_GEO_SPEED_JOIN_FILTER_TOPIC = "driver-violation-events";	
	private static final String SINK_DRIVER_AVG_SPEED_TOPIC= "driver-average-speed";
	

	
	public CalculateDriverAvgSpeedMicroService(Map<String, Object> kafkaConfigMap) {
		super(kafkaConfigMap, STREAMS_APP_ID );
		
		/* Override with Pojo Serde*/
		configureSerdes(configs, kafkaConfigMap);
		
		/* Setting up commit iterval as this is requierd so that aggregrations are continually written out as change logs. */
		configureCommitInterval();
	}
	




	public static void main(String[] args) {
		
		Map<String, Object> consumerConfig = createKafkaConfiguration(args);
		CalculateDriverAvgSpeedMicroService speedingTruckDriversApp = new CalculateDriverAvgSpeedMicroService(consumerConfig);
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

        /* Consume from driver-violation-events stream*/
        final KStream<String, TruckGeoSpeedJoin> geoSpeedJoinFilteredStream = 
        		builder.stream(SOURCE_GEO_SPEED_JOIN_FILTER_TOPIC,
        						Consumed.with(new Serdes.StringSerde(), new TruckGeoSpeedJoinSerde()));		

        /* Create Tumbling Window of driver speeds */
        KTable<Windowed<String>, DriverSpeedRunningCountAndSum> driverSpeedSumAndCountTable = 
        		createDriverSpeedWindow(geoSpeedJoinFilteredStream);
        
        /* Calculate average of the speeds in each tumbling window */
		KTable<Windowed<String>, DriverSpeedAvgValue> driverAvgSpeedTable = 
				calculateAverageOfDriverSpeedWindow(driverSpeedSumAndCountTable);
        
        /* Write averages to the driver-average-speed topic */
		KStream<Windowed<String>, DriverSpeedAvgValue> driverAvgSpeedStream =  driverAvgSpeedTable.toStream();
		driverAvgSpeedStream.to(SINK_DRIVER_AVG_SPEED_TOPIC, 
								Produced.with(new WindowSerde(), new DriverSpeedAvgValueSerde()));
        
		
		/* Build Topology */
		Topology streamsTopology = builder.build();

		LOGGER.debug("Truck-Calculate-Driver-Avg-Speed-Micro-Service Topoogy is: " 
				+ streamsTopology.describe());
		
		/* Create Streams App */
		KafkaStreams speedingDriversStreamsApps = new KafkaStreams(streamsTopology, configs);
		return speedingDriversStreamsApps;
	}





	private KTable<Windowed<String>, DriverSpeedAvgValue> calculateAverageOfDriverSpeedWindow(
			KTable<Windowed<String>, DriverSpeedRunningCountAndSum> driverSpeedSumAndCountTable) {
		//Calculate Average on the aggregation table
		ValueMapper<DriverSpeedRunningCountAndSum, DriverSpeedAvgValue> averageMapper = 
				new ValueMapper<DriverSpeedRunningCountAndSum, DriverSpeedAvgValue>() {

			@Override
			public DriverSpeedAvgValue apply(DriverSpeedRunningCountAndSum driverSpeedSumAndCountValue) {
				double average = 
						driverSpeedSumAndCountValue.getRunningSum() / 
						driverSpeedSumAndCountValue.getRunningCount();
				
				long eventTimeLong = new Date().getTime();
				return new DriverSpeedAvgValue(driverSpeedSumAndCountValue.getDriverId(), 
											   driverSpeedSumAndCountValue.getDriverName(), 
											   driverSpeedSumAndCountValue.getRoute(), 
											   average, eventTimeLong);
			}
		};
		
		
		KTable<Windowed<String>, DriverSpeedAvgValue> driverAvgSpeedTable = 
				driverSpeedSumAndCountTable.mapValues(averageMapper);
		return driverAvgSpeedTable;
	}





	private KTable<Windowed<String>, DriverSpeedRunningCountAndSum> createDriverSpeedWindow(
			final KStream<String, TruckGeoSpeedJoin> filteredStream) {
		//first group by driverId
		KGroupedStream<String, TruckGeoSpeedJoin> groupedStream = filteredStream.groupByKey();
		
		//Create a three minute window
		TimeWindows window = TimeWindows.of(TimeUnit.MINUTES.toMillis(3));
		window.until(TimeUnit.MINUTES.toMillis(3));
		TimeWindowedKStream<String, TruckGeoSpeedJoin> windowStream = groupedStream.windowedBy(window);
		
		//Perform running-count aggregation over window for average speed calculation
		Initializer<DriverSpeedRunningCountAndSum> avgInitializer = 
				new Initializer<DriverSpeedRunningCountAndSum>() {
		
			@Override
			public DriverSpeedRunningCountAndSum apply() {
				return new DriverSpeedRunningCountAndSum();
			}
		};	
		
		KTable<Windowed<String>, DriverSpeedRunningCountAndSum> driverSpeedSumAndCountTable = 
				windowStream.aggregate(avgInitializer, 
									   new DriverAvgSpeedAgregrator(),
									   Materialized.<String, DriverSpeedRunningCountAndSum, 
									   				WindowStore<Bytes,byte[]>>
												  as("time-windowed-aggregated-stream-store")
								       .withValueSerde(new DriverSpeedRunningCountAndSumSerde())
								        );
		return driverSpeedSumAndCountTable;
	}		
	
	
	private void configureCommitInterval() {
		// Configure interval the smae as the 3 minute window
		configs.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, 180000);
	}	
	
}	


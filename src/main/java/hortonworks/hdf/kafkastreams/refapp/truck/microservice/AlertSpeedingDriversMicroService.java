package hortonworks.hdf.kafkastreams.refapp.truck.microservice;

import hortonworks.hdf.kafkastreams.refapp.BaseStreamsApp;
import hortonworks.hdf.kafkastreams.refapp.truck.aggregrator.DriverSpeedAvgValue;
import hortonworks.hdf.kafkastreams.refapp.truck.serde.DriverSpeedAvgValueSerde;

import java.util.Map;
import java.util.concurrent.CountDownLatch;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Predicate;
import org.apache.kafka.streams.kstream.Produced;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class AlertSpeedingDriversMicroService extends BaseStreamsApp {

	private static final Logger LOGGER = LoggerFactory.getLogger(AlertSpeedingDriversMicroService.class);			
	
	private static final String STREAMS_APP_ID = "truck-micro-service-truck-alert-speeding-drivers";
	
	private static final String SOURCE_DRIVER_AVG_SPEED_TOPIC = "driver-average-speed";	
	private static final String SINK_ALERTS_SPEEDING_DRIVER_TOPIC= "alerts-speeding-drivers";

	protected static final double HIGH_SPEED = 80;
	

	
	public AlertSpeedingDriversMicroService(Map<String, Object> kafkaConfigMap) {
		super(kafkaConfigMap, STREAMS_APP_ID );
		
	}


	public static void main(String[] args) {
		
		Map<String, Object> consumerConfig = createKafkaConfiguration(args);
		AlertSpeedingDriversMicroService speedingTruckDriversApp = new AlertSpeedingDriversMicroService(consumerConfig);
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

        /* Consume from driver-average-speed stream*/
        final KStream<String, DriverSpeedAvgValue> driverAvgSpeedTableStream = 
        		builder.stream(SOURCE_DRIVER_AVG_SPEED_TOPIC, 
        					  Consumed.with(new Serdes.StringSerde(), new DriverSpeedAvgValueSerde()));		

		/* Filter the Stream for speeding drivers */
		KStream<String, DriverSpeedAvgValue> speedingDriversStream = 
				filterStreamForSpeedingDrivers(driverAvgSpeedTableStream);        
              
        /* Write the Speeding Drivers to the alerts-speeding-drivers topic */
		speedingDriversStream.to(SINK_ALERTS_SPEEDING_DRIVER_TOPIC, 
								  Produced.with(new Serdes.StringSerde(), new DriverSpeedAvgValueSerde()));
        
		
		/* Build Topology */
		Topology streamsTopology = builder.build();

		LOGGER.debug("Alert-Speeding-Drivers-Micro-Service Topoogy is: " 
				+ streamsTopology.describe());
		
		/* Create Streams App */
		KafkaStreams speedingDriversStreamsApps = new KafkaStreams(streamsTopology, configs);
		return speedingDriversStreamsApps;
	}


	private KStream<String, DriverSpeedAvgValue> filterStreamForSpeedingDrivers(
					KStream<String, DriverSpeedAvgValue> driverAvgSpeedStream) {
		
		Predicate<String, DriverSpeedAvgValue> predicate = 
				new Predicate<String, DriverSpeedAvgValue>() {
		
			@Override
			public boolean test(String key, DriverSpeedAvgValue value) {
				LOGGER.info("AVerage speed that filter be applied is: " + value);
				return  value != null && value.getSpeed_AVG() > HIGH_SPEED;
			}
		};
		KStream<String, DriverSpeedAvgValue> speedingDriversStream = 
				driverAvgSpeedStream.filter(predicate);
		
		return speedingDriversStream;
	}	
}	


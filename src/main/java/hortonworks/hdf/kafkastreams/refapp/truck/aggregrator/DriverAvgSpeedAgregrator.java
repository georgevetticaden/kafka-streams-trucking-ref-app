package hortonworks.hdf.kafkastreams.refapp.truck.aggregrator;

import hortonworks.hdf.kafkastreams.refapp.truck.dto.TruckGeoSpeedJoin;

import org.apache.kafka.streams.kstream.Aggregator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DriverAvgSpeedAgregrator implements Aggregator<String, TruckGeoSpeedJoin, DriverSpeedRunningCountAndSum> {
	
	private static final Logger LOGGER = LoggerFactory.getLogger(DriverAvgSpeedAgregrator.class);

	@Override
	public DriverSpeedRunningCountAndSum apply(String key, TruckGeoSpeedJoin value,
			DriverSpeedRunningCountAndSum aggregate) {
			
		DriverSpeedRunningCountAndSum newAggregation =  new DriverSpeedRunningCountAndSum(value.getDriverId(), value.getDriverName(), 
									  	aggregate.getRunningCount() + 1, aggregate.getRunningSum() + value.getSpeed());
		
		//LOGGER.debug("New Avg Aggregtion Value: " + newAggregation.toString());
		return newAggregation;
	}

}

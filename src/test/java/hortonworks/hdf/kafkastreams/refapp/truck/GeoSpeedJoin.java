package hortonworks.hdf.kafkastreams.refapp.truck;

import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import hortonworks.hdf.kafkastreams.refapp.truck.dto.TruckGeoSpeedJoin;
import hortonworks.hdf.schema.refapp.trucking.TruckGeoEventEnriched;
import hortonworks.hdf.schema.refapp.trucking.TruckSpeedEventEnriched;

public class GeoSpeedJoin {
	
	private static final Logger LOG = LoggerFactory.getLogger(GeoSpeedJoin.class);
	
	@Test
	public void geoSpeedJoinDTO() {
		TruckGeoEventEnriched geoStream = new TruckGeoEventEnriched();
		geoStream.correlationId=1;
		geoStream.driverId=1;
		geoStream.driverName="George";
				
				
		TruckSpeedEventEnriched speedStream = new TruckSpeedEventEnriched();
		speedStream.driverId=1;
		speedStream.speed=100;
		TruckGeoSpeedJoin join = new TruckGeoSpeedJoin(geoStream, speedStream);
		
		LOG.debug("Output is: " + geoStream.toString());
		
	}

}

package hortonworks.hdf.kafkastreams.refapp.truck.dto;

import org.apache.commons.lang.builder.ReflectionToStringBuilder;

import hortonworks.hdf.schema.refapp.trucking.TruckGeoEventEnriched;
import hortonworks.hdf.schema.refapp.trucking.TruckSpeedEventEnriched;

public class TruckGeoSpeedJoin {
	
	  /* This field is required for the Hive Kafka druid index service */
	  private long __time;
	  private String geoEventTime;
	  private long geoEventTimeLong;
	  private String eventSource;
	  private int truckId;
	  private int driverId;
	  private String driverName;
	  private int routeId;
	  private String route;
	  private String eventType;
	  private double latitude;
	  private double longitude;
	  private long correlationId;
	  private String geoAddress;
	  
	  private String speedEventTime;
	  private long speedEventTimeLong;
	  private int speed;

	public TruckGeoSpeedJoin(TruckGeoEventEnriched geoStream, TruckSpeedEventEnriched speedStream) {
		this.geoEventTime = String.valueOf(geoStream.getEventTime());
		this.geoEventTimeLong = geoStream.getEventTimeLong();
		this.eventSource = String.valueOf(geoStream.getEventSource());
		this.truckId = geoStream.getTruckId();
		this.driverId = geoStream.getDriverId();
		this.driverName = String.valueOf(geoStream.getDriverName());
		this.routeId = geoStream.getRouteId();
		this.route = String.valueOf(geoStream.getRoute());
		this.eventType = String.valueOf(geoStream.getEventType());
		this.latitude = geoStream.getLatitude();
		this.longitude = geoStream.getLongitude();
		this.correlationId = geoStream.getCorrelationId();
		this.geoAddress = String.valueOf(geoStream.getGeoAddress());
		
		this.speedEventTime = String.valueOf(speedStream.getEventTime());
		this.speedEventTimeLong = speedStream.getEventTimeLong();
		this.speed = speedStream.getSpeed();
		
		this.__time = geoStream.getEventTimeLong();
	}

	public String getGeoEventTime() {
		return geoEventTime;
	}

	public long getGeoEventTimeLong() {
		return geoEventTimeLong;
	}

	public String getEventSource() {
		return eventSource;
	}

	public int getTruckId() {
		return truckId;
	}

	public int getDriverId() {
		return driverId;
	}

	public String getDriverName() {
		return driverName;
	}

	public int getRouteId() {
		return routeId;
	}

	public String getRoute() {
		return route;
	}

	public String getEventType() {
		return eventType;
	}

	public double getLatitude() {
		return latitude;
	}

	public double getLongitude() {
		return longitude;
	}

	public long getCorrelationId() {
		return correlationId;
	}

	public String getGeoAddress() {
		return geoAddress;
	}

	public String getSpeedEventTime() {
		return speedEventTime;
	}

	public long getSpeedEventTimeLong() {
		return speedEventTimeLong;
	}

	public int getSpeed() {
		return speed;
	}

	@Override
	public String toString() {
		return ReflectionToStringBuilder.toString(this);
	}

	public long get__time() {
		return __time;
	}

	


}

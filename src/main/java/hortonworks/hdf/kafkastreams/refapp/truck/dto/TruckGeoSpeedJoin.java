package hortonworks.hdf.kafkastreams.refapp.truck.dto;

import org.apache.commons.lang.builder.ReflectionToStringBuilder;

import hortonworks.hdf.schema.refapp.trucking.TruckGeoEventEnriched;
import hortonworks.hdf.schema.refapp.trucking.TruckSpeedEventEnriched;

public class TruckGeoSpeedJoin {
	
	  /* This field is required for the Hive Kafka druid index service. Fields need to be lower case for the Hive/Kafka/Druid indexing service to work */
	  private long __time;
	  private String geoeventtime;
	  private long geoeventtimelong;
	  private String eventsource;
	  private int truckid;
	  private int driverid;
	  private String drivername;
	  private int routeid;
	  private String route;
	  private String eventtype;
	  private double latitude;
	  private double longitude;
	  private long correlationid;
	  private String geoaddress;
	  
	  private String speedeventtime;
	  private long speedeventtimelong;
	  private int speed;
	  
	

	public TruckGeoSpeedJoin() {

	}

	public TruckGeoSpeedJoin(long __time, String geoEventTime,
			long geoEventTimeLong, String eventSource, int truckId,
			int driverId, String driverName, int routeId, String route,
			String eventType, double latitude, double longitude,
			long correlationId, String geoAddress, String speedEventTime,
			long speedEventTimeLong, int speed) {
		super();
		this.__time = __time;
		this.geoeventtime = geoEventTime;
		this.geoeventtimelong = geoEventTimeLong;
		this.eventsource = eventSource;
		this.truckid = truckId;
		this.driverid = driverId;
		this.drivername = driverName;
		this.routeid = routeId;
		this.route = route;
		this.eventtype = eventType;
		this.latitude = latitude;
		this.longitude = longitude;
		this.correlationid = correlationId;
		this.geoaddress = geoAddress;
		this.speedeventtime = speedEventTime;
		this.speedeventtimelong = speedEventTimeLong;
		this.speed = speed;
	}

	public TruckGeoSpeedJoin(TruckGeoEventEnriched geoStream, TruckSpeedEventEnriched speedStream) {
		this.geoeventtime = String.valueOf(geoStream.getEventTime());
		this.geoeventtimelong = geoStream.getEventTimeLong();
		this.eventsource = String.valueOf(geoStream.getEventSource());
		this.truckid = geoStream.getTruckId();
		this.driverid = geoStream.getDriverId();
		this.drivername = String.valueOf(geoStream.getDriverName());
		this.routeid = geoStream.getRouteId();
		this.route = String.valueOf(geoStream.getRoute());
		this.eventtype = String.valueOf(geoStream.getEventType());
		this.latitude = geoStream.getLatitude();
		this.longitude = geoStream.getLongitude();
		this.correlationid = geoStream.getCorrelationId();
		this.geoaddress = String.valueOf(geoStream.getGeoAddress());
		
		this.speedeventtime = String.valueOf(speedStream.getEventTime());
		this.speedeventtimelong = speedStream.getEventTimeLong();
		this.speed = speedStream.getSpeed();
		
		this.__time = geoStream.getEventTimeLong();
	}




	public long get__time() {
		return __time;
	}

	public String getGeoeventtime() {
		return geoeventtime;
	}

	public long getGeoeventtimelong() {
		return geoeventtimelong;
	}

	public String getEventsource() {
		return eventsource;
	}

	public int getTruckid() {
		return truckid;
	}

	public int getDriverid() {
		return driverid;
	}

	public String getDrivername() {
		return drivername;
	}

	public int getRouteid() {
		return routeid;
	}

	public String getRoute() {
		return route;
	}

	public String getEventtype() {
		return eventtype;
	}

	public double getLatitude() {
		return latitude;
	}

	public double getLongitude() {
		return longitude;
	}

	public long getCorrelationid() {
		return correlationid;
	}

	public String getGeoaddress() {
		return geoaddress;
	}

	public String getSpeedeventtime() {
		return speedeventtime;
	}

	public long getSpeedeventtimelong() {
		return speedeventtimelong;
	}

	public int getSpeed() {
		return speed;
	}

	@Override
	public String toString() {
		return ReflectionToStringBuilder.toString(this);
	}


}

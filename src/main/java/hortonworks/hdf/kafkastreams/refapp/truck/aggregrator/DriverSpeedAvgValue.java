package hortonworks.hdf.kafkastreams.refapp.truck.aggregrator;

import org.apache.commons.lang3.builder.ReflectionToStringBuilder;

public class DriverSpeedAvgValue {
	
	private Integer driverid;
	private String drivername;
	private String route;
	private double speed_avg;
	private long processingtime;
   
	/* This field is required for the Hive Kafka druid index service. Fields need to be lower case for the Hive/Kafka/Druid indexing service to work */
	private long __time;	
	
	public DriverSpeedAvgValue() {

	}


	public DriverSpeedAvgValue(Integer driverId, String driverName, String route,
			double average, long processingTime) {
		super();
		this.driverid = driverId;
		this.drivername = driverName;
		this.route = route;
		this.speed_avg = average;
		this.processingtime = processingTime;
		this.__time = processingTime;
	}


	public Integer getDriverid() {
		return driverid;
	}


	public String getDrivername() {
		return drivername;
	}


	public String getRoute() {
		return route;
	}


	public double getSpeed_avg() {
		return speed_avg;
	}


	public long getProcessingtime() {
		return processingtime;
	}


	@Override
	public String toString() {
		return ReflectionToStringBuilder.toString(this);
	}


	public long get__time() {
		return __time;
	}

	
}

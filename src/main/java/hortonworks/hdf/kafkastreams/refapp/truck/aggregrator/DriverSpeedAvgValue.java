package hortonworks.hdf.kafkastreams.refapp.truck.aggregrator;

import org.apache.commons.lang3.builder.ReflectionToStringBuilder;

public class DriverSpeedAvgValue {
	
	private Integer driverId;
	private String driverName;
	private String route;
	private double speed_AVG;
	private long processingTime;
	
	public DriverSpeedAvgValue() {

	}


	public DriverSpeedAvgValue(Integer driverId, String driverName, String route,
			double average, long processingTime) {
		super();
		this.driverId = driverId;
		this.driverName = driverName;
		this.route = route;
		this.speed_AVG = average;
		this.processingTime = processingTime;
	}
	

	public Integer getDriverId() {
		return driverId;
	}

	public String getDriverName() {
		return driverName;
	}




	public double getSpeed_AVG() {
		return speed_AVG;
	}


	public long getProcessingTime() {
		return processingTime;
	}


	@Override
	public String toString() {
		return ReflectionToStringBuilder.toString(this);
	}


	public String getRoute() {
		return route;
	}


	public void setRoute(String route) {
		this.route = route;
	}


	
}

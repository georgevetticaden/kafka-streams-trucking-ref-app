package hortonworks.hdf.kafkastreams.refapp.truck.aggregrator;

import org.apache.commons.lang3.builder.ReflectionToStringBuilder;

public class DriverSpeedAvgValue {
	
	private Integer driverId;
	private String driverName;
	private double speed_AVG;
	private long processingTime;
	
	public DriverSpeedAvgValue(Integer driverId, String driverName,
			double average, long processingTime) {
		super();
		this.driverId = driverId;
		this.driverName = driverName;
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


}

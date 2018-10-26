package hortonworks.hdf.kafkastreams.refapp.truck.aggregrator;

import org.apache.commons.lang3.builder.ReflectionToStringBuilder;

public class DriverSpeedAvgValue {
	
	private Integer driverId;
	private String driverName;
	private double average;
	
	public DriverSpeedAvgValue(Integer driverId, String driverName,
			double average) {
		super();
		this.driverId = driverId;
		this.driverName = driverName;
		this.average = average;
	}
	

	public Integer getDriverId() {
		return driverId;
	}

	public String getDriverName() {
		return driverName;
	}

	
	public double getAverage() {
		return average;
	}


	@Override
	public String toString() {
		return ReflectionToStringBuilder.toString(this);
	}


}

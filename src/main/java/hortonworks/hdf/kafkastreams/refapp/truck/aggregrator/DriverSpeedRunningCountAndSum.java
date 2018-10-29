package hortonworks.hdf.kafkastreams.refapp.truck.aggregrator;

import org.apache.commons.lang3.builder.ReflectionToStringBuilder;

public class DriverSpeedRunningCountAndSum {
	
	private Integer driverId = -1;
	private String driverName = "";
	private String route;
	private int runningCount = 0;
	private int runningSum = 0;
	
	public DriverSpeedRunningCountAndSum(Integer driverId, String driverName, String route,
			int runningCount, int runningSum) {
		super();
		this.driverId = driverId;
		this.driverName = driverName;
		this.route = route;
		this.runningCount = runningCount;
		this.runningSum = runningSum;
	}
	
	public DriverSpeedRunningCountAndSum() {
		runningCount = 0;
		runningSum = 0;
	}

	public Integer getDriverId() {
		return driverId;
	}

	public String getDriverName() {
		return driverName;
	}

	public int getRunningCount() {
		return runningCount;
	}

	public int getRunningSum() {
		return runningSum;
	}
	
	
	@Override
	public String toString() {
		return ReflectionToStringBuilder.toString(this);
	}

	public String getRoute() {
		return route;
	}


	
}

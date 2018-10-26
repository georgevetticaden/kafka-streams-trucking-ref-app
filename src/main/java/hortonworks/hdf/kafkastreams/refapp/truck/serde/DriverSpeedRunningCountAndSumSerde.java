package hortonworks.hdf.kafkastreams.refapp.truck.serde;

import hortonworks.hdf.kafkastreams.refapp.truck.aggregrator.DriverSpeedRunningCountAndSum;

import java.util.Map;

import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serializer;

public class DriverSpeedRunningCountAndSumSerde implements Serde<DriverSpeedRunningCountAndSum> {

	@Override
	public void configure(Map<String, ?> configs, boolean isKey) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void close() {
		// TODO Auto-generated method stub
		
	}

	@Override
	public Serializer<DriverSpeedRunningCountAndSum> serializer() {
		return new JsonPOJOSerializer<DriverSpeedRunningCountAndSum>();
	}

	@Override
	public Deserializer<DriverSpeedRunningCountAndSum> deserializer() {
		return new JsonPOJODeserializer<DriverSpeedRunningCountAndSum>(DriverSpeedRunningCountAndSum.class);
	}



}

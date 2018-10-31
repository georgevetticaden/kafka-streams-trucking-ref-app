package hortonworks.hdf.kafkastreams.refapp.truck.serde;

import java.util.Map;

import hortonworks.hdf.kafkastreams.refapp.truck.aggregrator.DriverSpeedAvgValue;

import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serializer;

public class DriverSpeedAvgValueSerde implements Serde<DriverSpeedAvgValue> {

	@Override
	public void configure(Map<String, ?> configs, boolean isKey) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void close() {
		// TODO Auto-generated method stub
		
	}

	@Override
	public Serializer<DriverSpeedAvgValue> serializer() {
		return new JsonPOJOSerializer<DriverSpeedAvgValue>();
	}

	@Override
	public Deserializer<DriverSpeedAvgValue> deserializer() {
		return new JsonPOJODeserializer<DriverSpeedAvgValue>(DriverSpeedAvgValue.class);
	}

}

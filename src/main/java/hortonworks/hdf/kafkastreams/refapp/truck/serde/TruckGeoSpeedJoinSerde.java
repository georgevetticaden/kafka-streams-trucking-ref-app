package hortonworks.hdf.kafkastreams.refapp.truck.serde;

import hortonworks.hdf.kafkastreams.refapp.truck.dto.TruckGeoSpeedJoin;

import java.util.Map;

import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serializer;

public class TruckGeoSpeedJoinSerde implements Serde<TruckGeoSpeedJoin> {

	@Override
	public void configure(Map<String, ?> configs, boolean isKey) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void close() {
		// TODO Auto-generated method stub
		
	}

	@Override
	public Serializer<TruckGeoSpeedJoin> serializer() {
		return new JsonPOJOSerializer<TruckGeoSpeedJoin>();
	}

	@Override
	public Deserializer<TruckGeoSpeedJoin> deserializer() {
		return new JsonPOJODeserializer<TruckGeoSpeedJoin>(TruckGeoSpeedJoin.class);
	}

}

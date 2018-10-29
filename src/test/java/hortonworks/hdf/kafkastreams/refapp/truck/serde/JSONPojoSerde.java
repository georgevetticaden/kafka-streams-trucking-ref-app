package hortonworks.hdf.kafkastreams.refapp.truck.serde;

import hortonworks.hdf.kafkastreams.refapp.truck.aggregrator.DriverSpeedRunningCountAndSum;

import org.apache.kafka.streams.kstream.Window;
import org.apache.kafka.streams.kstream.Windowed;
import org.junit.Test;

public class JSONPojoSerde {
	
	@Test
	public void serde() {
		DriverSpeedRunningCountAndSum pojo = new DriverSpeedRunningCountAndSum(1, "George", "Route 1",  1, 1);
		JsonPOJOSerializer<DriverSpeedRunningCountAndSum> serializer = new JsonPOJOSerializer<DriverSpeedRunningCountAndSum>();
		byte[] pojoSerialized = serializer.serialize("test", pojo);
		
		JsonPOJODeserializer<DriverSpeedRunningCountAndSum> deserializer = new JsonPOJODeserializer<DriverSpeedRunningCountAndSum>(DriverSpeedRunningCountAndSum.class);
		DriverSpeedRunningCountAndSum deserializedPojo = deserializer.deserialize("test", pojoSerialized);
		System.out.println(deserializedPojo.toString());
	}
	
//	@Test
//	public void serde2() {
//		
//		Windowed<String> windowed = new Windowed<String>("test", null);
//		WindowSerde serde = new WindowSerde();
//		byte[] pojoSerialized = serde.serializer().serialize("test", windowed);
//		
//		System.out.println(pojoSerialized);
//		
//	}	

}

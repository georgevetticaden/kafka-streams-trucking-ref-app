package hortonworks.hdf.kafkastreams.refapp.truck.serde;

import java.util.Map;

import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.streams.kstream.Windowed;
import org.joda.time.DateTime;

public class WindowSerde implements Serde<Windowed<String>> {
	
	

	@Override
	public void configure(Map<String, ?> configs, boolean isKey) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void close() {
		// TODO Auto-generated method stub
		
	}

	@Override
	public Serializer<Windowed<String>> serializer() {
		return new Serializer<Windowed<String>>() {

			@Override
			public void configure(Map<String, ?> configs, boolean isKey) {
				// TODO Auto-generated method stub
				
			}

			@Override
			public byte[] serialize(String topic, Windowed<String> data) {
				
				StringBuffer buffer = new StringBuffer();
				buffer.append("windowStart: ");
				buffer.append(new DateTime( data.window().start()).toString());
				buffer.append(", end time: ");
				buffer.append(new DateTime(data.window().end()).toString());
				return buffer.toString().getBytes();
			}

			@Override
			public void close() {
				// TODO Auto-generated method stub
				
			}
		};
	}

	@Override
	public Deserializer<Windowed<String>> deserializer() {
		// TODO Auto-generated method stub
		return null;
	}

}

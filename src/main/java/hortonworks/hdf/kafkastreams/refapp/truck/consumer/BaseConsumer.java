package hortonworks.hdf.kafkastreams.refapp.truck.consumer;

import static net.sourceforge.argparse4j.impl.Arguments.store;

import java.util.Collections;
import java.util.Properties;

import net.sourceforge.argparse4j.ArgumentParsers;
import net.sourceforge.argparse4j.inf.ArgumentParser;
import net.sourceforge.argparse4j.inf.ArgumentParserException;
import net.sourceforge.argparse4j.inf.Namespace;

import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;

import com.hortonworks.registries.schemaregistry.client.SchemaRegistryClient;
import com.hortonworks.registries.schemaregistry.serdes.avro.AbstractAvroSnapshotDeserializer;
import com.hortonworks.registries.schemaregistry.serdes.avro.kafka.KafkaAvroDeserializer;


public abstract class BaseConsumer {


	protected static Properties getConsumerConfigs(Namespace result) {
		Properties props = new Properties();
		props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, result.getString("bootstrap.servers"));
		props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, result.getString("auto.offset.reset"));
		props.put(ConsumerConfig.GROUP_ID_CONFIG, result.getString("groupId"));
		props.put(ConsumerConfig.CLIENT_ID_CONFIG, result.getString("clientId"));
		props.put(ConsumerConfig.MAX_PARTITION_FETCH_BYTES_CONFIG, result.getString("max.partition.fetch.bytes"));

		props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true");
		
		/* Setup all the required config/props for the Schema Registry Deserailizer */
		configureSRDeserializers(props, result);
    
        /* If talking to secure Kafka cluster, set security protocol as "SASL_PLAINTEXT */
        if("SASL_PLAINTEXT".equals(result.getString("security.protocol"))) {
		 	props.put(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, "SASL_PLAINTEXT");  
		 	props.put("sasl.kerberos.service.name", "kafka");        	
        }
		
		return props;
	}
	
	private static void configureSRDeserializers(Properties props, Namespace result) {
		props.put(SchemaRegistryClient.Configuration.SCHEMA_REGISTRY_URL.name(), result.getString("schema.registry.url"));
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, KafkaAvroDeserializer.class);        
        props.put(AbstractAvroSnapshotDeserializer.SPECIFIC_AVRO_READER, true);        
	}		
	
	
	protected static Properties createKafkaConfiguration(String[] args) {
		
		ArgumentParser parser = argParser();
		Properties configs = null;
		try {
			
			Namespace result = parser.parseArgs(args);
			configs = getConsumerConfigs(result);			
		} catch (ArgumentParserException e) {
			if(args.length == 0)
				parser.printHelp();
			else 
				parser.handleError(e);
			System.exit(0);
		}	
		return configs;

	}		


	/**
	 * Get the command-line argument parser.
	 */
	protected static ArgumentParser argParser() {
		ArgumentParser parser = ArgumentParsers
				.newArgumentParser("basic-consumer-loop")
				.defaultHelp(true)
				.description("This example demonstrates kafka consumer auto subscription capabilities");

		parser.addArgument("--bootstrap.servers").action(store())
				.required(true)
				.type(String.class)
				.help("comma separated broker list");
		
		parser.addArgument("--schema.registry.url").action(store())
				.required(true)
				.type(String.class)
				.help("Schema Registry url...");		

		parser.addArgument("--groupId").action(store())
				.required(true)
				.type(String.class)
				.help("Group Identifier");

		parser.addArgument("--clientId").action(store())
				.required(true)
				.type(String.class)
				.help("Client Identifier");

		parser.addArgument("--auto.offset.reset").action(store())
				.required(false)
				.setDefault("latest")
				.type(String.class)
				.choices("earliest", "latest")
				.help("What to do when there is no initial offset in Kafka");

		parser.addArgument("--max.partition.fetch.bytes").action(store())
				.required(false)
				.setDefault("1024")
				.type(String.class)
				.help("The maximum amount of data per-partition the server will return");
		
		parser.addArgument("--security.protocol").action(store())
				.required(false)
				.setDefault("PLAINTEXT")
				.type(String.class)
				.help("Either PLAINTEXT or SASL_PLAINTEXT");
		


		return parser;
	}
	
	

	

	
	
	
}

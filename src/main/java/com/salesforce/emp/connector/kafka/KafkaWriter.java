package com.salesforce.emp.connector.kafka;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Arrays;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

public class KafkaWriter {
	Producer<String, String> producer;
	String topic;
	//private static final Logger log = (Logger) LoggerFactory.getLogger(KafkaWriter.class);

	
	public KafkaWriter() {
		topic = "sfdc_contact_topic";// config.consumerTopic;
		Properties kafka_config = getKafkaConfig();
		this.producer = new KafkaProducer<>(kafka_config);
		
	}


	public void write(String value) throws InterruptedException, ExecutionException {
		ProducerRecord<String, String> pr = new ProducerRecord<String, String>(topic, value.toString());
		Future<RecordMetadata> res = producer.send(pr);
		RecordMetadata m = res.get();
		System.out.println("Msg sent to "+m.topic() + " offset - "+m.offset() + " Part - "+m.partition());
		
	}

	private Properties getKafkaConfig() {

		Properties kafka_config = new Properties();
		String kafka_host = "kafka-dev-broker0.cloudera.sin3-l1x7.cloudera.site:9093,kafka-dev-broker2.cloudera.sin3-l1x7.cloudera.site:9093,kafka-dev-broker1.cloudera.sin3-l1x7.cloudera.site:9093";
		String truststore_location = "/home/spark/venkat/client.trustore.jks";
	    String kafka_truststore_password = "clouderadev";//"clouderadev";
	    String kafka_ssl_config="org.apache.kafka.common.security.plain.PlainLoginModule required username=\"csso_vjonnalagadda\" password=\"Venkat@123\";";
	    //String kafka_ssl_config="org.apache.kafka.common.security.plain.PlainLoginModule required username=\"srv_web-dev-research\" password=\"Research@123\";";
	  
		String bootstrapServers_list[] = kafka_host.split(",");
		List<String> bootstrapServers = Arrays.asList(bootstrapServers_list);
		//String username = config.username;
		//String password = config.password;
		//String module = "org.apache.kafka.common.security.scram.ScramLoginModule";
		//String jaasConfig = String.format("%s required username=\"%s\" password=\"%s\";", module, username, password);
		String module = "org.apache.kafka.common.security.plain.PlainLoginModule";
		String jaasConfig = String.format("%s required username=\"%s\" password=\"%s\";", module, "csso_vjonnalagadda", "Venkat@123");
		//String jaasConfig = String.format("%s required username=\"%s\" password=\"%s\";", module, "srv_forrweb_dev", "BX5Q{}ap:_|*");
		//org.apache.kafka.common.security.plain.PlainLoginModule required username="srv_forrweb_dev" password="BX5Q{}ap:_|*";
		
		
		kafka_config.put("bootstrap.servers", bootstrapServers);
		kafka_config.put("key.serializer", StringSerializer.class.getName());
		kafka_config.put("value.serializer", StringSerializer.class.getName());
		kafka_config.put("ssl.truststore.type", "jks");
	    kafka_config.put("ssl.truststore.location", truststore_location);
		kafka_config.put("ssl.truststore.password", kafka_truststore_password);
		kafka_config.put("sasl.mechanism", "SCRAM-SHA-256");
		kafka_config.put("security.protocol", "SASL_SSL");
		kafka_config.put("sasl.jaas.config", jaasConfig);
		kafka_config.put("linger.ms", 5);

		//return kafka_config;
		Properties props = new Properties();
	   	 props.put("bootstrap.servers", "kafka-dev-broker2.cloudera.sin3-l1x7.cloudera.site:9093,kafka-dev-broker1.cloudera.sin3-l1x7.cloudera.site:9093,kafka-dev-broker0.cloudera.sin3-l1x7.cloudera.site:9093\r\n" + 
	   	 		"");
	   	 props.put("acks", "all");
	   	 props.put("retries", 1);
	   	 props.put("batch.size", 16384);
	   	 props.put("linger.ms", 1);
	   	 props.put("buffer.memory", 33554432);
	   	 props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
	   	 props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
	   	 props.put("security.protocol","SASL_SSL");
	   	 props.put("sasl.mechanism","PLAIN");
	   	 props.put("ssl.truststore.location","client.truststore.jks");
	   	 props.put("ssl.truststore.password","cloudera");
	   	 //props.put("sasl.jaas.config","org.apache.kafka.common.security.plain.PlainLoginModule required username=\"srv_web-dev-research\" password=\"Research@123\";");
	   	 //props.put("sasl.jaas.config","org.apache.kafka.common.security.plain.PlainLoginModule required username=\"srv_forrweb_dev\" password=\"BX5Q{}ap:_|*\";");    	    
	   	 props.put("sasl.jaas.config","org.apache.kafka.common.security.plain.PlainLoginModule required username=\"csso_vjonnalagadda\" password=\"Venkat@123\";");    	    
	
		return props;
	}

}


/*
 kafka_host = "kafka-dev-broker0.cloudera.sin3-l1x7.cloudera.site:9093,kafka-dev-broker2.cloudera.sin3-l1x7.cloudera.site:9093,kafka-dev-broker1.cloudera.sin3-l1x7.cloudera.site:9093"
  truststore_location = "/dbfs/dbfs/mnt/cloudera_kafka/truststore_dev_temp.jks"
  kafka_truststore_password = "clouderadev"
  kafka_ssl_config="""org.apache.kafka.common.security.plain.PlainLoginModule required username="srv_web-dev-research" password="Research@123";"""
  
  
  
  #kafka_host = dbutils.secrets.get(kafka_scope,'kafka_ssl_url')
  #keystore_location= dbutils.secrets.get(kafka_scope,'kafka_keystore_location')
  #truststore_location =  dbutils.secrets.get(kafka_scope,'kafka_truststore_location')
  #kafka_key_password =  dbutils.secrets.get(kafka_scope,'kafka_key_password')
  #kafka_keystore_password =  dbutils.secrets.get(kafka_scope,'kafka_keystore_password')
  #kafka_truststore_password =  dbutils.secrets.get(kafka_scope,'kafka_truststore_password')
  
  df.write.format("kafka")\
            .option("kafka.bootstrap.servers",kafka_host)\
            #.option("kafka.ssl.key.password",kafka_keystore_password )\
            #.option("kafka.ssl.keystore.type", "jks")\
            #.option("kafka.ssl.keystore.location", keystore_location)\
            #.option("kafka.ssl.keystore.password",kafka_keystore_password )\
            .option("kafka.ssl.truststore.type", "jks")\
            .option("kafka.ssl.truststore.location",truststore_location)\
            .option("kafka.ssl.truststore.password",kafka_truststore_password )\
            .option("kafka.security.protocol", "SASL_SSL")\
            .option("kafka.linger.ms", "2000")\
            .option("kafka.batch.size", "30000")\
            .option("kafka.request.timeout.ms", "60000")\
            .option("topic",topic)\
            .save()
 * */

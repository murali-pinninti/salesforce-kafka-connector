package com.salesforce.emp.connector.kafka;

import java.util.Arrays;
import java.util.List;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.ExecutionException;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.common.serialization.StringDeserializer;  
import org.slf4j.Logger;  
import org.slf4j.LoggerFactory;  
  
import java.time.Duration;  
import java.util.Collections;
import java.util.Date; 
public class KafkaReader {
	String topic = "sfdc_contact_topic";;
	//private static final Logger log = (Logger) LoggerFactory.getLogger(KafkaWriter.class);

	
	public KafkaReader() {
		topic = "sfdc_contact_topic";// config.consumerTopic;
		Properties kafka_config = getKafkaConfig();
		
	}
	
	private static Properties getKafkaConfig() {

		Properties kafka_config = new Properties();
		String kafka_host = "kafka-dev-broker0.cloudera.sin3-l1x7.cloudera.site:9093,kafka-dev-broker2.cloudera.sin3-l1x7.cloudera.site:9093,kafka-dev-broker1.cloudera.sin3-l1x7.cloudera.site:9093";
	    String truststore_location = "/home/spark/cloudera_kafka/client.trustore.jks";
	    String kafka_truststore_password = "clouderadev";//"clouderadev";
	    String kafka_ssl_config="org.apache.kafka.common.security.plain.PlainLoginModule required username=\"csso_vjonnalagadda\" password=\"Venkat@123\";";
	  
		String bootstrapServers_list[] = kafka_host.split(",");
		List<String> bootstrapServers = Arrays.asList(bootstrapServers_list);
		//String username = config.username;
		//String password = config.password;
		//String module = "org.apache.kafka.common.security.scram.ScramLoginModule";
		//String jaasConfig = String.format("%s required username=\"%s\" password=\"%s\";", module, username, password);
		String module = "org.apache.kafka.common.security.plain.PlainLoginModule";
		String jaasConfig = String.format("%s required username=\"%s\" password=\"%s\";", module, "csso_vjonnalagadda", "Venkat@123");

		//org.apache.kafka.common.security.plain.PlainLoginModule required username="srv_forrweb_dev" password="BX5Q{}ap:_|*";
		kafka_config.put("bootstrap.servers", bootstrapServers);
		kafka_config.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,   StringDeserializer.class.getName());  
		kafka_config.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,StringDeserializer.class.getName());
		kafka_config.put("ssl.truststore.type", "jks");
		kafka_config.put("ssl.truststore.location", truststore_location);
		kafka_config.put("ssl.truststore.password", kafka_truststore_password);
		kafka_config.put("sasl.mechanism", "SCRAM-SHA-256");
		kafka_config.put("security.protocol", "SASL_SSL");
		kafka_config.put("sasl.jaas.config", jaasConfig);
		kafka_config.put("linger.ms", 5);
		kafka_config.setProperty(ConsumerConfig.GROUP_ID_CONFIG,"grp1");  
		kafka_config.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,"earliest");

		//return kafka_config;
		
		 Properties props = new Properties();
	   	 props.put("bootstrap.servers", "kafka-dev-broker2.cloudera.sin3-l1x7.cloudera.site:9093,kafka-dev-broker1.cloudera.sin3-l1x7.cloudera.site:9093,kafka-dev-broker0.cloudera.sin3-l1x7.cloudera.site:9093\r\n" + 
	   	 		"");
	   	 props.put("acks", "all");
	   	 props.put("retries", 0);
	   	 props.put("batch.size", 16384);
	   	 props.put("linger.ms", 1);
	   	 props.put("buffer.memory", 33554432);
	   	 props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
	   	 props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
	   	 props.put("security.protocol","SASL_SSL");
	   	 props.put("sasl.mechanism","PLAIN");
	   	 props.put("ssl.truststore.location","client.truststore.jks");
	   	 props.put("ssl.truststore.password","cloudera");
	   	 props.put("sasl.jaas.config","org.apache.kafka.common.security.plain.PlainLoginModule required username=\"csso_vjonnalagadda\" password=\"Venkat@123\";");
	    //props.put("sasl.jaas.config","org.apache.kafka.common.security.plain.PlainLoginModule required username=\"srv_forrweb_dev\" password=\"BX5Q{}ap:_|*\";");    	    
	   	 //props.put("sasl.jaas.config","org.apache.kafka.common.security.plain.PlainLoginModule required username=\"srv_web_producer_dev\" password=\"i/2zI1$?},p1\";");    	    
	   	props.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,   StringDeserializer.class.getName());  
	   	props.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,StringDeserializer.class.getName());
	   	props.setProperty(ConsumerConfig.GROUP_ID_CONFIG,UUID.randomUUID().toString());  
	   	props.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,"earliest");
	   	 /*Producer<String, String> producer = new KafkaProducer<>(props);
	   	 for (int i = 1; i < 4; i++)
	   	     producer.send(new ProducerRecord<String, String>("web_contact_access_group_dev", Integer.toString(i), Integer.toString(i)));
	
	   	 producer.close();*/
	   	 return props;
	}
	public static void main(String[] args) {
		// TODO Auto-generated method stub
		KafkaConsumer<String,String> consumer= new KafkaConsumer<String,String>(getKafkaConfig());  
        //Subscribing  
		System.out.println("Consumer created");
        consumer.subscribe(Arrays.asList("sfdc_contact_topic"));  
        System.out.println("Topic subscribed");
        //polling  
        while(true){  
            ConsumerRecords<String,String> records=consumer.poll(Duration.ofMillis(100));  
            //System.out.println("Polling "+new Date().getTime());
            
            for(ConsumerRecord<String,String> record: records){  
                System.out.println("Key: "+ record.key() + ", Value:" +record.value());  
                System.out.println("Partition:" + record.partition()+",Offset:"+record.offset());  
            }  
  
  
        }  
	}

}

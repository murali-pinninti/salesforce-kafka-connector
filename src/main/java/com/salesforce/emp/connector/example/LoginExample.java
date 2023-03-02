/*
 * Copyright (c) 2016, salesforce.com, inc.
 * All rights reserved.
 * Licensed under the BSD 3-Clause license.
 * For full license text, see LICENSE.TXT file in the repo root  or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.emp.connector.example;

import static com.salesforce.emp.connector.LoginHelper.login;

import java.net.URL;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import org.eclipse.jetty.util.ajax.JSON;
import org.json.JSONObject;

import com.salesforce.emp.connector.BayeuxParameters;
import com.salesforce.emp.connector.EmpConnector;
import com.salesforce.emp.connector.LoginHelper;
import com.salesforce.emp.connector.TopicSubscription;
import com.salesforce.emp.connector.kafka.KafkaWriter;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

/**
 * An example of using the EMP connector using login credentials
 *
 * @author hal.hildebrand
 * @since API v37.0
 */
public class LoginExample {

    // More than one thread can be used in the thread pool which leads to parallel processing of events which may be acceptable by the application
    // The main purpose of asynchronous event processing is to make sure that client is able to perform /meta/connect requests which keeps the session alive on the server side
    private static final ExecutorService workerThreadPool = Executors.newFixedThreadPool(1);

    public static void main(String[] argv) throws Exception {
        if (argv.length < 3 || argv.length > 4) {
            System.err.println("Usage: LoginExample username password topic [replayFrom]");
            System.exit(1);
        }
        long replayFrom = EmpConnector.REPLAY_FROM_TIP;
        if (argv.length == 4) {
            replayFrom = Long.parseLong(argv[3]);
        }
        System.out.println(argv[0]+" "+argv[1]+" "+argv[2] );

        BearerTokenProvider tokenProvider = new BearerTokenProvider(() -> {
            try {
                return login(argv[0], argv[1]);
            } catch (Exception e) {
                e.printStackTrace(System.err);
                System.exit(1);
                throw new RuntimeException(e);
            }
        });

        BayeuxParameters params = tokenProvider.login();
        
        KafkaWriter kafkaWriter = new KafkaWriter();

        Consumer<Map<String, Object>> consumer = 
        		event -> workerThreadPool.submit(() -> {
        			String res = JSON.toString(event);
        			try {
        				//contactParse(res);
        				System.out.println("Sending to kafka");
        				kafkaWriter.write(res);
        			} catch(Exception e) {
        				e.printStackTrace();
        			}
        			
        			System.out.println(String.format("parReceived--:\n%s", res));
        			System.out.println("PARSING-"+res);
        		});
        		//System.out.println(String.format("Received:\n%s", JSON.toString(event))));

        EmpConnector connector = new EmpConnector(params);

        connector.setBearerTokenProvider(tokenProvider);

        connector.start().get(5, TimeUnit.SECONDS);

        TopicSubscription subscription = connector.subscribe(argv[2], replayFrom, consumer).get(5, TimeUnit.SECONDS);

        System.out.println(String.format("Subscribed: %s", subscription));
    }
    
    public static final void contactParse(String json) {
		JSONObject obj = new JSONObject(json);
		JSONObject event = obj.getJSONObject("event");
		JSONObject feed = obj.getJSONObject("sobject");
		String type = event.get("type").toString();
		System.out.println("PARSED data type- "+type);
		System.out.println(type);
		if("created".equalsIgnoreCase(type)) {
			String id = feed.get("Id").toString();
			String accountId = feed.get("AccountId").toString();
			String firstName = feed.get("FirstName").toString();
			String lastName = feed.get("LastName").toString();
			String email = feed.get("Email").toString();
			String query = "INSERT INTO DWDEV.CONTACTS_KAFKA VALUES('"+id+"','"+accountId+"','"
						+firstName+"','"+lastName+"','"+email+"')";
			System.out.println("Query formed \n"+query);
			Util.sql(query);
		}
	}
}

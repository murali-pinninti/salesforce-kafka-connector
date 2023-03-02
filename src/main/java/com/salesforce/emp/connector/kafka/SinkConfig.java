/*
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.salesforce.emp.connector.kafka;


import java.util.Map;

import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;

public class SinkConfig extends AbstractConfig {


	public static final String MAX_BATCH = "max.batch.records";
	private static final int MAX_BATCH_DEFAULT = 1;
	private static final String MAX_BATCH_DOC = "number of records to be sent in a single API call";
	private static final String MAX_BATCH_DISPLAY = "max records in a batch";

	public static final String CONNECTION_GROUP = "Connection";
	public static final String KAFKA_CLIENT_GROUP = "Connection";
	private static final String RETRIES_GROUP = "Retries";

	public static final String BOOTSTRAP_SERVERS = "kafka.server";
	private static final String BOOTSTRAP_SERVERS_DOC = "kafka server host url";
	private static final String BOOTSTRAP_SERVERS_DISPLAY = "kafka server url";
	private static final String BOOTSTRAP_SERVERS_DEFAULT = "";

	public static final String USERNAME_KAFKA = "kafka.sasl.username";
	private static final String USERNAME_KAFKA_DOC = "kafka SASL username";
	private static final String USERNAME_KAFKA_DISPLAY = "kafka SASL username(from eventador console)";
	private static final String USERNAME_KAFKA_DEFAULT = "";

	public static final String PASSWORD_KAFKA = "kafka.sasl.password";
	private static final String PASSWORD_KAFKA_DOC = "kafka SASL password";
	private static final String PASSWORD_KAFKA_DISPLAY = "kafka SASL password (from eventador console)";
	private static final String PASSWORD_KAFKA_DEFAULT = "";

	public static final String TRUSTSTORE_PATH_KAFKA = "truststore.location";
	private static final String TRUSTSTORE_PATH_KAFKA_DOC = "truststore jks file location on disc";
	private static final String TRUSTSTORE_PATH_KAFKA_DISPLAY = "truststore jks file location on disc";
	private static final String TRUSTSTORE_PATH_KAFKA_DEFAULT = "";

	public static final String TRUSTSTORE_PASSWORD_KAFKA = "truststore.password";
	private static final String TRUSTSTORE_PASSWORD_KAFKA_DOC = "truststore jks password";
	private static final String TRUSTSTORE_PASSWORD_KAFKA_DISPLAY = "password used in creating jks file";
	private static final String TRUSTSTORE_PASSWORD_KAFKA_DEFAULT = "";

	public static final String CONSUMER_TOPIC = "consumer.topic";
	private static final String CONSUMER_TOPIC_DOC = "kafka topic to write data ";
	private static final String CONSUMER_TOPIC_DISPLAY = "kafka topic to write data ";
	private static final String CONSUMER_TOPIC_DEFAULT = "";

	

	private static final ConfigDef.Range NON_NEGATIVE_INT_VALIDATOR = ConfigDef.Range.atLeast(0);

	public static final ConfigDef CONFIG_DEF = new ConfigDef()

			.define(BOOTSTRAP_SERVERS, ConfigDef.Type.STRING, BOOTSTRAP_SERVERS_DEFAULT, ConfigDef.Importance.HIGH,
					BOOTSTRAP_SERVERS_DOC, KAFKA_CLIENT_GROUP, 1, ConfigDef.Width.SHORT, BOOTSTRAP_SERVERS_DISPLAY)
			.define(USERNAME_KAFKA, ConfigDef.Type.STRING, USERNAME_KAFKA_DEFAULT, ConfigDef.Importance.HIGH,
					USERNAME_KAFKA_DOC, KAFKA_CLIENT_GROUP, 2, ConfigDef.Width.SHORT, USERNAME_KAFKA_DISPLAY)
			.define(PASSWORD_KAFKA, ConfigDef.Type.STRING, PASSWORD_KAFKA_DEFAULT, ConfigDef.Importance.HIGH,
					PASSWORD_KAFKA_DOC, KAFKA_CLIENT_GROUP, 3, ConfigDef.Width.SHORT, PASSWORD_KAFKA_DISPLAY)
			.define(TRUSTSTORE_PATH_KAFKA, ConfigDef.Type.STRING, TRUSTSTORE_PATH_KAFKA_DEFAULT,
					ConfigDef.Importance.HIGH, TRUSTSTORE_PATH_KAFKA_DOC, KAFKA_CLIENT_GROUP, 4, ConfigDef.Width.SHORT,
					TRUSTSTORE_PATH_KAFKA_DISPLAY)
			.define(TRUSTSTORE_PASSWORD_KAFKA, ConfigDef.Type.STRING, TRUSTSTORE_PASSWORD_KAFKA_DEFAULT,
					ConfigDef.Importance.HIGH, TRUSTSTORE_PASSWORD_KAFKA_DOC, KAFKA_CLIENT_GROUP, 5,
					ConfigDef.Width.SHORT, TRUSTSTORE_PASSWORD_KAFKA_DISPLAY)
			.define(CONSUMER_TOPIC, ConfigDef.Type.STRING, CONSUMER_TOPIC_DEFAULT, ConfigDef.Importance.HIGH,
					CONSUMER_TOPIC_DOC, KAFKA_CLIENT_GROUP, 6, ConfigDef.Width.SHORT, CONSUMER_TOPIC_DISPLAY)

			.define(MAX_BATCH, ConfigDef.Type.INT, MAX_BATCH_DEFAULT, NON_NEGATIVE_INT_VALIDATOR,
					ConfigDef.Importance.MEDIUM, MAX_BATCH_DOC, RETRIES_GROUP, 1, ConfigDef.Width.SHORT,
					MAX_BATCH_DISPLAY);

	public final int maxBatch;
	public String bootstrapServers;
	public String username;
	public String password;
	public String truststore_file;
	public String truststore_password;
	public String consumerTopic;

	public SinkConfig(Map<?, ?> props) {
		super(CONFIG_DEF, props);
		maxBatch = getInt(MAX_BATCH);

		this.bootstrapServers = getString(BOOTSTRAP_SERVERS);
		this.username = getString(USERNAME_KAFKA);
		this.password = getString(PASSWORD_KAFKA);
		this.truststore_file = getString(TRUSTSTORE_PATH_KAFKA);
		this.truststore_password = getString(TRUSTSTORE_PASSWORD_KAFKA);
		this.consumerTopic = getString(CONSUMER_TOPIC);
	}


	public static void main(String... args) {
		System.out.println(CONFIG_DEF.toRst());
	}
}

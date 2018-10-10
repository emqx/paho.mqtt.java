package emq.paho.mqtt5.support.test;

import java.text.MessageFormat;
import java.util.logging.Logger;

import org.eclipse.paho.mqttv5.client.IMqttDeliveryToken;
import org.eclipse.paho.mqttv5.client.IMqttToken;
import org.eclipse.paho.mqttv5.client.MqttAsyncClient;
import org.eclipse.paho.mqttv5.client.MqttConnectionOptions;
import org.eclipse.paho.mqttv5.client.test.SubscribeTests;
import org.eclipse.paho.mqttv5.client.test.logging.LoggingUtilities;
import org.eclipse.paho.mqttv5.common.MqttException;
import org.eclipse.paho.mqttv5.common.MqttMessage;
import org.eclipse.paho.mqttv5.common.MqttSubscription;
import org.eclipse.paho.mqttv5.common.packet.MqttProperties;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import emq.paho.mqtt5.support.test.utils.ConnPropertiesUtil;
import emq.paho.mqtt5.support.test.utils.EMQMqttV5Receiver;
import emq.paho.mqtt5.support.test.utils.TestClientUtilities;
import emq.paho.mqtt5.support.test.utils.Utility;

/*
 * Prerequisites: set mqtt.max_topic_alias = 10 in emqx.conf
 */
/*
 * Finished
 */
public class TopicAliasTest {
	
	private static final Logger log = Logger.getLogger(TopicAliasTest.class.getName());
	
	@BeforeClass
	public static void setUpBeforeClass() throws Exception {
		TestHelper.setUpBeforeClass();
	}
	
	@Test
	public void testTopicAliasSupport() throws MqttException, InterruptedException {
		String clientId = Utility.getMethodName();
		int timeout = 120 * 1000;
		
		LoggingUtilities.banner(log, SubscribeTests.class, clientId);
		String topic = TestHelper.getTopicPrefix() + clientId;
		
		EMQMqttV5Receiver mqttV5Receiver = new EMQMqttV5Receiver(clientId, LoggingUtilities.getPrintStream());
		MqttConnectionOptions connOptions = new MqttConnectionOptions();
		connOptions.setTopicAliasMaximum(10);
		MqttAsyncClient asyncClient = TestClientUtilities.connectAndGetClient(TestHelper.getServerURI().toString(), clientId,
				mqttV5Receiver, connOptions, timeout);
		
		for (int qos = 0; qos <= 2; qos++) {
			int topicAlias = qos + 1;
			MqttProperties properties = new MqttProperties();
			properties.setTopicAlias(topicAlias);
			
			// Subscribe to a topic
			log.info(MessageFormat.format("Subscribing to topic: {0} at QoS {1}", topic, qos));
			MqttSubscription subscription = new MqttSubscription(topic, qos);
			IMqttToken subscribeToken = asyncClient.subscribe(subscription);
			subscribeToken.waitForCompletion(timeout);
			
			// Publish a message to the topic and set up the topic alias
			String messagePayload = "Test Payload at QoS : " + qos;
			MqttMessage testMessage = new MqttMessage(messagePayload.getBytes(), qos, false, properties);
			log.info(MessageFormat.format("Publishing Message {0} to: {1} at QoS: {2} with topic alias {3}", 
					testMessage.toDebugString(), topic, qos, topicAlias));
			IMqttDeliveryToken deliveryToken = asyncClient.publish(topic, testMessage);
			deliveryToken.waitForCompletion(timeout);

			log.info("Waiting for delivery and validating message.");
			boolean received = mqttV5Receiver.validateReceipt(topic, qos, testMessage, 30);
			Assert.assertTrue(received);
			
			// Publish a message to the topic using topic alias
			messagePayload = MessageFormat.format("[Topic alias] Test Payload at QoS : {0}", qos);
			testMessage = new MqttMessage(messagePayload.getBytes(), qos, false, properties);
			log.info(MessageFormat.format("Publishing Message {0} to: {1} at QoS: {2} using topic alias {3}", 
					testMessage.toDebugString(), topic, qos, topicAlias));
			deliveryToken = asyncClient.publish("", testMessage);
			deliveryToken.waitForCompletion(timeout);

			log.info("Waiting for delivery and validating message.");
			received = mqttV5Receiver.validateReceipt(topic, qos, testMessage, 30);
			Assert.assertTrue(received);
		}
		
		TestClientUtilities.disconnectAndCloseClient(asyncClient, 5000);
	}
	
	@Test
	public void testRemapTopicAlias() throws MqttException, InterruptedException {
		String clientId = Utility.getMethodName();
		int timeout = 120 * 1000;
		
		LoggingUtilities.banner(log, SubscribeTests.class, clientId);
		String topic = TestHelper.getTopicPrefix() + clientId;
		
		EMQMqttV5Receiver mqttV5Receiver = new EMQMqttV5Receiver(clientId, LoggingUtilities.getPrintStream());
		MqttConnectionOptions connOptions = new MqttConnectionOptions();
		connOptions.setTopicAliasMaximum(10);
		MqttAsyncClient asyncClient = TestClientUtilities.connectAndGetClient(TestHelper.getServerURI().toString(), clientId,
				mqttV5Receiver, connOptions, timeout);
		
		for (int qos = 0; qos <= 2; qos++) {
			int topicAlias = qos + 1;
			MqttProperties properties = new MqttProperties();
			properties.setTopicAlias(topicAlias);
			
			// Subscribe to 2 topics
			log.info(MessageFormat.format("Subscribing to topic: {0} at QoS {1}", topic, qos));
			MqttSubscription subscription = new MqttSubscription(topic, qos);
			IMqttToken subscribeToken = asyncClient.subscribe(subscription);
			subscribeToken.waitForCompletion(timeout);
			
			String newTopic = topic + "-new";
			log.info(MessageFormat.format("Subscribing to topic: {0} at QoS {1}", newTopic, qos));
			subscription = new MqttSubscription(newTopic, qos);
			subscribeToken = asyncClient.subscribe(subscription);
			subscribeToken.waitForCompletion(timeout);
			
			// Publish a message to the 1st topic and set up the topic alias
			String messagePayload = "Test Payload at QoS : " + qos;
			MqttMessage testMessage = new MqttMessage(messagePayload.getBytes(), qos, false, properties);
			log.info(MessageFormat.format("Publishing Message {0} to: {1} at QoS: {2} with topic alias {3}", 
					testMessage.toDebugString(), topic, qos, topicAlias));
			IMqttDeliveryToken deliveryToken = asyncClient.publish(topic, testMessage);
			deliveryToken.waitForCompletion(timeout);

			log.info("Waiting for delivery and validating message.");
			boolean received = mqttV5Receiver.validateReceipt(topic, qos, testMessage);
			Assert.assertTrue(received);
			log.info(MessageFormat.format("Message received at topic {0}.", topic));
			
			// Publish a message to the 2nd topic using the same topic alias
			log.info(MessageFormat.format("Publishing Message {0} to: {1} at QoS: {2} with topic alias {3}", 
					testMessage.toDebugString(), newTopic, qos, topicAlias));
			deliveryToken = asyncClient.publish(newTopic, testMessage);
			deliveryToken.waitForCompletion(timeout);
			
			log.info("Waiting for delivery and validating message.");
			received = mqttV5Receiver.validateReceipt(newTopic, qos, testMessage);
			Assert.assertTrue(received);
			log.info(MessageFormat.format("Message received at topic {0}.", newTopic));
			
			// Publish a message to the topic using topic alias
			messagePayload = MessageFormat.format("[Topic alias] Test Payload at QoS : {0}", qos);
			testMessage = new MqttMessage(messagePayload.getBytes(), qos, false, properties);
			log.info(MessageFormat.format("Publishing Message {0} to: {1} at QoS: {2} using topic alias {3}", 
					testMessage.toDebugString(), newTopic, qos, topicAlias));
			deliveryToken = asyncClient.publish("", testMessage);
			deliveryToken.waitForCompletion(timeout);

			// Only the 2nd topic delivers message
			log.info("Waiting for delivery and validating message.");
			received = mqttV5Receiver.validateReceipt(newTopic, qos, testMessage, 10);
			Assert.assertTrue(received);
			log.info(MessageFormat.format("Message received at topic {0} using topic alias.", newTopic));
			received = mqttV5Receiver.validateReceipt(topic, qos, testMessage, 10);
			Assert.assertFalse(received);
			log.info(MessageFormat.format("No message received at topic {0} using topic alias.", topic));
		}
		
		TestClientUtilities.disconnectAndCloseClient(asyncClient, 5000);
	}
	
	@Test
	public void testInvalidTopicAlias() throws MqttException, InterruptedException {
		String clientId = Utility.getMethodName();
		int timeout = 120 * 1000;
		
		LoggingUtilities.banner(log, SubscribeTests.class, clientId);
		String topic = TestHelper.getTopicPrefix() + clientId;
		
		EMQMqttV5Receiver mqttV5Receiver = new EMQMqttV5Receiver(clientId, LoggingUtilities.getPrintStream());
		MqttConnectionOptions connOptions = new MqttConnectionOptions();
		connOptions.setTopicAliasMaximum(10);
		MqttAsyncClient asyncClient = TestClientUtilities.connectAndGetClient(TestHelper.getServerURI().toString(), clientId,
				mqttV5Receiver, connOptions, timeout);
		
		try {
			int topicAliasMax = ConnPropertiesUtil.getTopicAliasMaximum(asyncClient);
			
			for (int qos = 0; qos <= 2; qos++) {
				// Subscribe to the topic
				log.info(MessageFormat.format("Subscribing to topic: {0} at QoS {1}", topic, qos));
				MqttSubscription subscription = new MqttSubscription(topic, qos);
				IMqttToken subscribeToken = asyncClient.subscribe(subscription);
				subscribeToken.waitForCompletion(timeout);
				
				MqttProperties properties = new MqttProperties();
				// Publish a message to the topic with an invalid topic alias
				int topicAlias = 0;
				properties.setTopicAlias(topicAlias);
				String messagePayload = "Test Payload at QoS : " + qos;
				MqttMessage testMessage = new MqttMessage(messagePayload.getBytes(), qos, false, properties);
				log.info(MessageFormat.format("Publishing Message {0} to: {1} at QoS: {2} with topic alias {3}", 
						testMessage.toDebugString(), topic, qos, topicAlias));
				IMqttDeliveryToken deliveryToken = asyncClient.publish(topic, testMessage);
				try {
					deliveryToken.waitForCompletion(timeout);
					
					log.info("Waiting for delivery and validating message.");
					boolean received = mqttV5Receiver.validateReceipt(topic, qos, testMessage, 10);
					Assert.assertFalse(received);
					log.info(MessageFormat.format("No message received at topic {0}.", topic));
					
					if (!mqttV5Receiver.isConnected()) {
						//reconnect
						asyncClient = TestClientUtilities.connectAndGetClient(TestHelper.getServerURI().toString(), clientId,
								mqttV5Receiver, connOptions, timeout);
					}
				} catch (MqttException e) {
					if (!mqttV5Receiver.isConnected()) {
						//reconnect
						asyncClient = TestClientUtilities.connectAndGetClient(TestHelper.getServerURI().toString(), clientId,
								mqttV5Receiver, connOptions, timeout);
					}
				}
				
				// Publish a message to the topic with an invalid topic alias
				topicAlias = topicAliasMax + 1;
				properties.setTopicAlias(topicAlias);
				messagePayload = "Test Payload at QoS : " + qos;
				testMessage = new MqttMessage(messagePayload.getBytes(), qos, false, properties);
				log.info(MessageFormat.format("Publishing Message {0} to: {1} at QoS: {2}", 
						testMessage.toDebugString(), topic, qos));
				deliveryToken = asyncClient.publish(topic, testMessage);
				try {
					deliveryToken.waitForCompletion(timeout);
					
					log.info("Waiting for delivery and validating message.");
					boolean received = mqttV5Receiver.validateReceipt(topic, qos, testMessage, 10);
					Assert.assertFalse(received);
					log.info(MessageFormat.format("No message received at topic {0}.", topic));
					
					if (!mqttV5Receiver.isConnected()) {
						//reconnect
						asyncClient = TestClientUtilities.connectAndGetClient(TestHelper.getServerURI().toString(), clientId,
								mqttV5Receiver, connOptions, timeout);
					}
				} catch (MqttException e) {
					if (!mqttV5Receiver.isConnected()) {
						//reconnect
						asyncClient = TestClientUtilities.connectAndGetClient(TestHelper.getServerURI().toString(), clientId,
								mqttV5Receiver, connOptions, timeout);
					}
				}
			}
		} catch (Exception e) {
			e.printStackTrace();
			Assert.fail();
		}
		
		TestClientUtilities.disconnectAndCloseClient(asyncClient, 5000);
	}
	
	@Test
	public void testTopicAliasMaximum() throws MqttException, InterruptedException {
		String clientId = Utility.getMethodName();
		int timeout = 120 * 1000;
		
		LoggingUtilities.banner(log, SubscribeTests.class, clientId);
		String topic = TestHelper.getTopicPrefix() + clientId;
		
		EMQMqttV5Receiver mqttV5Receiver = new EMQMqttV5Receiver(clientId, LoggingUtilities.getPrintStream());
		//without setting topic alias maximum for receiver, broker will not send to subscriber.
		MqttAsyncClient asyncClient = TestClientUtilities.connectAndGetClient(TestHelper.getServerURI().toString(), clientId,
				mqttV5Receiver, null, timeout);
		
		for (int qos = 0; qos <= 2; qos++) {
			int topicAlias = qos + 1;
			MqttProperties properties = new MqttProperties();
			properties.setTopicAlias(topicAlias);
			
			// Subscribe to a topic
			log.info(MessageFormat.format("Subscribing to topic: {0} at QoS {1}", topic, qos));
			MqttSubscription subscription = new MqttSubscription(topic, qos);
			IMqttToken subscribeToken = asyncClient.subscribe(subscription);
			subscribeToken.waitForCompletion(timeout);
			
			// Publish a message to the topic and set up the topic alias
			String messagePayload = "Test Payload at QoS : " + qos;
			MqttMessage testMessage = new MqttMessage(messagePayload.getBytes(), qos, false, properties);
			log.info(MessageFormat.format("Publishing Message {0} to: {1} at QoS: {2} with topic alias {3}", 
					testMessage.toDebugString(), topic, qos, topicAlias));
			IMqttDeliveryToken deliveryToken = asyncClient.publish(topic, testMessage);
			deliveryToken.waitForCompletion(timeout);

			log.info("Waiting for delivery and validating message.");
			boolean received = mqttV5Receiver.validateReceipt(topic, qos, testMessage, 10);
			Assert.assertFalse(received);
			log.info(MessageFormat.format("No message received at topic {0}.", topic));
		}
		
		TestClientUtilities.disconnectAndCloseClient(asyncClient, 5000);
	}

}

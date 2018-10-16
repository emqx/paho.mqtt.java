package emq.paho.mqtt5.support.test;

import java.text.MessageFormat;
import java.util.logging.Logger;

import org.eclipse.paho.mqttv5.client.IMqttDeliveryToken;
import org.eclipse.paho.mqttv5.client.IMqttToken;
import org.eclipse.paho.mqttv5.client.MqttAsyncClient;
import org.eclipse.paho.mqttv5.client.MqttConnectionOptions;
import org.eclipse.paho.mqttv5.client.test.logging.LoggingUtilities;
import org.eclipse.paho.mqttv5.common.MqttException;
import org.eclipse.paho.mqttv5.common.MqttMessage;
import org.eclipse.paho.mqttv5.common.MqttSubscription;
import org.eclipse.paho.mqttv5.common.packet.MqttProperties;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import emq.paho.mqtt5.support.test.utils.EMQMqttV5Receiver;
import emq.paho.mqtt5.support.test.utils.TestClientUtilities;
import emq.paho.mqtt5.support.test.utils.Utility;

/*
 * Finished
 */
public class SubscriptionOptionsTest {
	
private static final Logger log = Logger.getLogger(SubscriptionOptionsTest.class.getName());
	
	@BeforeClass
	public static void setUpBeforeClass() throws Exception {
		TestHelper.setUpBeforeClass();
	}
	
	@Test
	public void testNoLocal() throws MqttException, InterruptedException {
		String clientId = Utility.getMethodName();
		int timeout = 120 * 1000;
		
		LoggingUtilities.banner(log, TopicAliasTest.class, clientId);
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
			
			// Subscribe to a topic with noLocal=false
			log.info(MessageFormat.format("Subscribing to topic: {0} at QoS {1}", topic, qos));
			MqttSubscription subscription = new MqttSubscription(topic, qos);
			subscription.setNoLocal(false);
			IMqttToken subscribeToken = asyncClient.subscribe(subscription);
			subscribeToken.waitForCompletion(timeout);
			
			// Publish a message to the topic using the same client id as subscriber
			String messagePayload = "Test Payload at QoS : " + qos;
			MqttMessage testMessage = new MqttMessage(messagePayload.getBytes(), qos, false, properties);
			log.info(MessageFormat.format("Publishing Message {0} to: {1} at QoS: {2}", 
					testMessage.toDebugString(), topic, qos));
			IMqttDeliveryToken deliveryToken = asyncClient.publish(topic, testMessage);
			deliveryToken.waitForCompletion(timeout);
			
			log.info("Waiting for delivery and validating message.");
			boolean received = mqttV5Receiver.validateReceipt(topic, qos, testMessage, 10);
			Assert.assertTrue(received);
			
			// Unsubscribe from the topic
			log.info("Unsubscribing from : " + topic);
			IMqttToken unsubscribeToken = asyncClient.unsubscribe(topic);
			unsubscribeToken.waitForCompletion(timeout);
			
			// Subscribe to a topic with noLocal=true
			log.info(MessageFormat.format("Subscribing to topic: {0} at QoS {1}", topic, qos));
			subscription = new MqttSubscription(topic, qos);
			subscription.setNoLocal(true);
			subscribeToken = asyncClient.subscribe(subscription);
			subscribeToken.waitForCompletion(timeout);
			
			// Publish a message to the topic using the same client id as subscriber
			log.info(MessageFormat.format("Publishing Message {0} to: {1} at QoS: {2}", 
					testMessage.toDebugString(), topic, qos));
			deliveryToken = asyncClient.publish(topic, testMessage);
			deliveryToken.waitForCompletion(timeout);
			
			log.info("Waiting for delivery and validating message.");
			received = mqttV5Receiver.validateReceipt(topic, qos, testMessage, 2);
			Assert.assertFalse(received);
			
			// Unsubscribe from the topic
			log.info("Unsubscribing from : " + topic);
			unsubscribeToken = asyncClient.unsubscribe(topic);
			unsubscribeToken.waitForCompletion(timeout);
			
			/*Should fail (Protocol Error) according to the spec, but haven't been implemented by EMQ yet*/
			// Shared subscription with noLocal=true
//			String sharedTopic = "$share/myshare/" + clientId;
//			log.info(MessageFormat.format("Subscribing to topic: {0} at QoS {1}", sharedTopic, qos));
//			subscription = new MqttSubscription(sharedTopic, qos);
//			subscription.setNoLocal(true);
//			subscribeToken = asyncClient.subscribe(subscription);
//			subscribeToken.waitForCompletion(timeout);
//			
//			// Unsubscribe from the topic
//			log.info("Unsubscribing from : " + topic);
//			unsubscribeToken = asyncClient.unsubscribe(topic);
//			unsubscribeToken.waitForCompletion(timeout);
		}
		
		TestClientUtilities.disconnectAndCloseClient(asyncClient, 5000);
	}
	
	/*
	 * how to test it on client side?
	 */
//	@Test
//	public void testRetainAsPublished() throws MqttException, InterruptedException {
//		String clientId = Utility.getMethodName();
//		int timeout = 120 * 1000;
//		
//		LoggingUtilities.banner(log, TopicAliasTest.class, clientId);
//		String topic = clientId;
//		
//		EMQMqttV5Receiver mqttV5Receiver = new EMQMqttV5Receiver(clientId, LoggingUtilities.getPrintStream());
//		MqttConnectionOptions connOptions = new MqttConnectionOptions();
//		connOptions.setTopicAliasMaximum(10);
//		MqttAsyncClient asyncClient = TestClientUtilities.connectAndGetClient(TestHelper.getServerURI().toString(), clientId,
//				mqttV5Receiver, connOptions, timeout);
//		
//		for (int qos = 0; qos <= 0; qos++) {
//			int topicAlias = qos + 1;
//			MqttProperties properties = new MqttProperties();
//			properties.setTopicAlias(topicAlias);
//			
//			// Subscribe to a topic with retain as published=false
//			log.info(MessageFormat.format("Subscribing to topic: {0} at QoS {1}", topic, qos));
//			MqttSubscription subscription = new MqttSubscription(topic, qos);
//			subscription.setRetainAsPublished(true);
//			IMqttToken subscribeToken = asyncClient.subscribe(subscription);
//			subscribeToken.waitForCompletion(timeout);
//			
//			// Publish a message to the topic using the same client id as subscriber
//			String messagePayload = "Test Payload at QoS : " + qos + " " + System.currentTimeMillis();
//			MqttMessage testMessage = new MqttMessage(messagePayload.getBytes(), qos, true, properties);
//			log.info(MessageFormat.format("Publishing Message {0} to: {1} at QoS: {2}", 
//					testMessage.toDebugString(), topic, qos));
//			IMqttDeliveryToken deliveryToken = asyncClient.publish(topic, testMessage);
//			deliveryToken.waitForCompletion(timeout);
//			
////			log.info("Waiting for delivery and validating message.");
////			boolean received = mqttV5Receiver.validateReceipt(topic, qos, testMessage, 10);
////			Assert.assertTrue(received);
//			
//			// Unsubscribe from the topic
//			log.info("Unsubscribing from : " + topic);
//			IMqttToken unsubscribeToken = asyncClient.unsubscribe(topic);
//			unsubscribeToken.waitForCompletion(timeout);
//		}
//		
//		TestClientUtilities.disconnectAndCloseClient(asyncClient, 5000);
//	}

	@Test
	public void testRetainHandling() throws MqttException, InterruptedException {
		String clientId = Utility.getMethodName();
		int timeout = 120 * 1000;
		
		LoggingUtilities.banner(log, TopicAliasTest.class, clientId);
		String topic = clientId;
		
		EMQMqttV5Receiver mqttV5Receiver = new EMQMqttV5Receiver(clientId, LoggingUtilities.getPrintStream());
		MqttConnectionOptions connOptions = new MqttConnectionOptions();
		connOptions.setTopicAliasMaximum(10);
		MqttAsyncClient asyncClient = TestClientUtilities.connectAndGetClient(TestHelper.getServerURI().toString(), clientId,
				mqttV5Receiver, connOptions, timeout);
		
		for (int qos = 0; qos <= 2; qos++) {
			int topicAlias = qos + 1;
			MqttProperties properties = new MqttProperties();
			properties.setTopicAlias(topicAlias);
			
			// Publish a retained message
			String messagePayload = "Test Payload for retained message at QoS : " + qos;
			MqttMessage testMessage = new MqttMessage(messagePayload.getBytes(), qos, true, properties);
			log.info(MessageFormat.format("Publishing Message {0} to: {1} at QoS: {2}", 
					testMessage.toDebugString(), topic, qos));
			IMqttDeliveryToken deliveryToken = asyncClient.publish(topic, testMessage);
			deliveryToken.waitForCompletion(timeout);
			
			// Subscribe to a topic with Retain Handling = 0
			log.info(MessageFormat.format("Subscribing to topic: {0} at QoS {1}", topic, qos));
			MqttSubscription subscription = new MqttSubscription(topic, qos);
			subscription.setRetainHandling(0);
			IMqttToken subscribeToken = asyncClient.subscribe(subscription);
			subscribeToken.waitForCompletion(timeout);
			
			log.info("Waiting for retained message and validating message.");
			boolean received = mqttV5Receiver.validateReceipt(topic, qos, testMessage, 10);
			Assert.assertTrue(received);
			
			// Subscribe again with Retain Handling = 0
			log.info(MessageFormat.format("Subscribing to topic: {0} at QoS {1}", topic, qos));
			subscription = new MqttSubscription(topic, qos);
			subscription.setRetainHandling(0);
			subscribeToken = asyncClient.subscribe(subscription);
			subscribeToken.waitForCompletion(timeout);
			
			// still receive the retained message
			log.info("Waiting for retained message and validating message.");
			received = mqttV5Receiver.validateReceipt(topic, qos, testMessage, 10);
			Assert.assertTrue(received);
			
			// Unsubscribe from the topic
			log.info("Unsubscribing from : " + topic);
			IMqttToken unsubscribeToken = asyncClient.unsubscribe(topic);
			unsubscribeToken.waitForCompletion(timeout);
			
			// Subscribe to a topic with Retain Handling = 1
			log.info(MessageFormat.format("Subscribing to topic: {0} at QoS {1}", topic, qos));
			subscription = new MqttSubscription(topic, qos);
			subscription.setRetainHandling(1);
			subscribeToken = asyncClient.subscribe(subscription);
			subscribeToken.waitForCompletion(timeout);
			
			log.info("Waiting for retained message and validating message.");
			received = mqttV5Receiver.validateReceipt(topic, qos, testMessage, 10);
			Assert.assertTrue(received);
			
			// Subscribe again with Retain Handling = 1
			log.info(MessageFormat.format("Subscribing to topic: {0} at QoS {1}", topic, qos));
			subscription = new MqttSubscription(topic, qos);
			subscription.setRetainHandling(1);
			subscribeToken = asyncClient.subscribe(subscription);
			subscribeToken.waitForCompletion(timeout);
			
			// cannot receive the retained message
			log.info("Waiting for retained message and validating message.");
			received = mqttV5Receiver.validateReceipt(topic, qos, testMessage, 10);
			Assert.assertFalse(received);
			
			// Unsubscribe from the topic
			log.info("Unsubscribing from : " + topic);
			unsubscribeToken = asyncClient.unsubscribe(topic);
			unsubscribeToken.waitForCompletion(timeout);
			
			// Subscribe to a topic with Retain Handling = 2
			log.info(MessageFormat.format("Subscribing to topic: {0} at QoS {1}", topic, qos));
			subscription = new MqttSubscription(topic, qos);
			subscription.setRetainHandling(2);
			subscribeToken = asyncClient.subscribe(subscription);
			subscribeToken.waitForCompletion(timeout);
			
			// cannot receive the retained message
			log.info("Waiting for retained message and validating message.");
			received = mqttV5Receiver.validateReceipt(topic, qos, testMessage, 10);
			Assert.assertFalse(received);
			
			// Unsubscribe from the topic
			log.info("Unsubscribing from : " + topic);
			unsubscribeToken = asyncClient.unsubscribe(topic);
			unsubscribeToken.waitForCompletion(timeout);
		}
		
		TestClientUtilities.disconnectAndCloseClient(asyncClient, 5000);
	}
}

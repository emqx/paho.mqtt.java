package emq.paho.mqtt5.support.test;

import java.text.MessageFormat;
import java.util.logging.Logger;

import org.eclipse.paho.mqttv5.client.IMqttDeliveryToken;
import org.eclipse.paho.mqttv5.client.IMqttToken;
import org.eclipse.paho.mqttv5.client.MqttAsyncClient;
import org.eclipse.paho.mqttv5.client.test.logging.LoggingUtilities;
import org.eclipse.paho.mqttv5.common.MqttException;
import org.eclipse.paho.mqttv5.common.MqttMessage;
import org.eclipse.paho.mqttv5.common.MqttSubscription;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import emq.paho.mqtt5.support.test.utils.EMQMqttV5Receiver;
import emq.paho.mqtt5.support.test.utils.TestClientUtilities;
import emq.paho.mqtt5.support.test.utils.Utility;

/*
 * Finished
 */
public class SharedSubscriptionTest {
	
private static final Logger log = Logger.getLogger(SharedSubscriptionTest.class.getName());
	
	@BeforeClass
	public static void setUpBeforeClass() throws Exception {
		TestHelper.setUpBeforeClass();
	}
	
	@Test
	public void testSharedSubscription() throws MqttException, InterruptedException {
		String clientId = Utility.getMethodName();
		int timeout = 120 * 1000;
		
		LoggingUtilities.banner(log, TopicAliasTest.class, clientId);
		String topic = TestHelper.getTopicPrefix() + clientId;
		String sharedTopic = "$share/group/" + topic;
		
		MqttAsyncClient pubClient = TestClientUtilities.connectAndGetClient(TestHelper.getServerURI().toString(), clientId + "-pub",
				null, null, timeout);
		
		// Build 2 connections for separate sub
		EMQMqttV5Receiver receiver1 = new EMQMqttV5Receiver(clientId, LoggingUtilities.getPrintStream());
		MqttAsyncClient subClient1 = TestClientUtilities.connectAndGetClient(TestHelper.getServerURI().toString(), clientId + "-sub1",
				receiver1, null, timeout);
		
		EMQMqttV5Receiver receiver2 = new EMQMqttV5Receiver(clientId, LoggingUtilities.getPrintStream());
		MqttAsyncClient subClient2 = TestClientUtilities.connectAndGetClient(TestHelper.getServerURI().toString(), clientId + "-sub2",
				receiver2, null, timeout);
		
		for (int qos = 0; qos <= 2; qos++) {
			// Single subscriber
			MqttSubscription subscription = new MqttSubscription(sharedTopic, qos);
			IMqttToken subscribeToken = subClient1.subscribe(subscription);
			subscribeToken.waitForCompletion(timeout);
			
			// Publish a message to the topic
			String messagePayload = "Helloworld";
			MqttMessage testMessage = new MqttMessage(messagePayload.getBytes(), qos, false, null);
			log.info(MessageFormat.format("Publishing Message {0} to: {1} at QoS: {2}", 
					testMessage.toDebugString(), topic, qos));
			IMqttDeliveryToken deliveryToken = pubClient.publish(topic, testMessage);
			deliveryToken.waitForCompletion(timeout);
			
			log.info("Waiting for delivery and validating message.");
			boolean received = receiver1.validateReceipt(sharedTopic, qos, testMessage, 10);
			Assert.assertTrue(received);
			
			// Unsubscribe from the topic
			log.info("Unsubscribing from : " + sharedTopic);
			IMqttToken unsubscribeToken = subClient1.unsubscribe(sharedTopic);
			unsubscribeToken.waitForCompletion(timeout);
			
			// 2 subscribers belongs to the same group
			subscription = new MqttSubscription(sharedTopic, qos);
			subscribeToken = subClient1.subscribe(subscription);
			subscribeToken.waitForCompletion(timeout);
			
			subscription = new MqttSubscription(sharedTopic, qos);
			subscribeToken = subClient2.subscribe(subscription);
			subscribeToken.waitForCompletion(timeout);
			
			// Publish a message to the topic
			messagePayload = "Helloworld";
			testMessage = new MqttMessage(messagePayload.getBytes(), qos, false, null);
			log.info(MessageFormat.format("Publishing Message {0} to: {1} at QoS: {2}", 
					testMessage.toDebugString(), topic, qos));
			deliveryToken = pubClient.publish(topic, testMessage);
			deliveryToken.waitForCompletion(timeout);
			
			log.info("Waiting for delivery and validating message.");
			boolean received1 = receiver1.validateReceipt(sharedTopic, qos, testMessage, 10);
			boolean received2 = receiver2.validateReceipt(sharedTopic, qos, testMessage, 10);
			Assert.assertNotEquals(received1, received2);
			
			// Unsubscribe from the topic
			log.info("Unsubscribing from : " + sharedTopic);
			unsubscribeToken = subClient1.unsubscribe(sharedTopic);
			unsubscribeToken.waitForCompletion(timeout);
			unsubscribeToken = subClient2.unsubscribe(sharedTopic);
			unsubscribeToken.waitForCompletion(timeout);
			
			// 2 subscribers belongs to different groups
			subscription = new MqttSubscription(sharedTopic, qos);
			subscribeToken = subClient1.subscribe(subscription);
			subscribeToken.waitForCompletion(timeout);
			
			String anotherSharedTopic = "$share/anothergroup/" + topic;
			subscription = new MqttSubscription(anotherSharedTopic, qos);
			subscribeToken = subClient2.subscribe(subscription);
			subscribeToken.waitForCompletion(timeout);
			
			// Publish a message to the topic
			messagePayload = "Helloworld";
			testMessage = new MqttMessage(messagePayload.getBytes(), qos, false, null);
			log.info(MessageFormat.format("Publishing Message {0} to: {1} at QoS: {2}", 
					testMessage.toDebugString(), topic, qos));
			deliveryToken = pubClient.publish(topic, testMessage);
			deliveryToken.waitForCompletion(timeout);
			
			log.info("Waiting for delivery and validating message.");
			received1 = receiver1.validateReceipt(sharedTopic, qos, testMessage, 10);
			Assert.assertTrue(received1);
			received2 = receiver2.validateReceipt(sharedTopic, qos, testMessage, 10);
			Assert.assertTrue(received2);
			
			// Unsubscribe from the topic
			log.info("Unsubscribing from : " + sharedTopic);
			unsubscribeToken = subClient1.unsubscribe(sharedTopic);
			unsubscribeToken.waitForCompletion(timeout);
			unsubscribeToken = subClient2.unsubscribe(anotherSharedTopic);
			unsubscribeToken.waitForCompletion(timeout);
		}
		
		TestClientUtilities.disconnectAndCloseClient(pubClient, 5000);
		TestClientUtilities.disconnectAndCloseClient(subClient1, 5000);
		TestClientUtilities.disconnectAndCloseClient(subClient2, 5000);
	}

}

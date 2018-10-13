package emq.paho.mqtt5.support.test;

import static org.junit.Assert.assertNotNull;

import java.text.MessageFormat;
import java.util.logging.Logger;

import org.eclipse.paho.mqttv5.client.IMqttDeliveryToken;
import org.eclipse.paho.mqttv5.client.IMqttToken;
import org.eclipse.paho.mqttv5.client.MqttAsyncClient;
import org.eclipse.paho.mqttv5.client.test.SubscribeTests;
import org.eclipse.paho.mqttv5.client.test.logging.LoggingUtilities;
import org.eclipse.paho.mqttv5.common.MqttException;
import org.eclipse.paho.mqttv5.common.MqttMessage;
import org.eclipse.paho.mqttv5.common.MqttSubscription;
import org.eclipse.paho.mqttv5.common.packet.MqttProperties;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import emq.paho.mqtt5.support.test.utils.EMQMqttV5Receiver;
import emq.paho.mqtt5.support.test.utils.EMQMqttV5Receiver.ReceivedMessage;
import emq.paho.mqtt5.support.test.utils.TestClientUtilities;
import emq.paho.mqtt5.support.test.utils.Utility;

/*
 * Finished
 */
public class SubscriptionIdTest {
	
	private static final Logger log = Logger.getLogger(SubscriptionIdTest.class.getName());
	
	@BeforeClass
	public static void setUpBeforeClass() throws Exception {
		TestHelper.setUpBeforeClass();
	}
	
	@Test
	public void testSubscriptionId() throws MqttException, InterruptedException {
		String clientId = Utility.getMethodName();
		int timeout = 120 * 1000;
		
		LoggingUtilities.banner(log, TopicAliasTest.class, clientId);
		String topic1 = clientId + "/" + TestHelper.getTopicPrefix();
		String topic2 = clientId + "/+"; 
		
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
			// Subscribe in subClient1 and subClient2 (each connection subscribe 1 topic)
			MqttProperties properties1 = new MqttProperties();
			int subId = qos * 10 + 1;
			properties1.setSubscriptionIdentifier(subId);
			MqttSubscription subscription = new MqttSubscription(topic1, qos);
			log.info(MessageFormat.format("Subscribing to topic: {0} at QoS {1} with subscription id {2}", 
					topic1, qos, subId));
			IMqttToken subscribeToken = subClient1.subscribe(new MqttSubscription[] {subscription}, null, null, properties1);
			subscribeToken.waitForCompletion(timeout);
			
			MqttProperties properties2 = new MqttProperties();
			subId = qos * 10 + 2;
			properties2.setSubscriptionIdentifier(subId);
			subscription = new MqttSubscription(topic2, qos);
			log.info(MessageFormat.format("Subscribing to topic: {0} at QoS {1} with subscription id {2}", 
					topic2, qos, subId));
			subscribeToken = subClient2.subscribe(new MqttSubscription[] {subscription}, null, null, properties2);
			subscribeToken.waitForCompletion(timeout);
			
			// Publish a message to the topic
			String messagePayload = "Test Payload at QoS : " + qos;
			MqttMessage testMessage = new MqttMessage(messagePayload.getBytes(), qos, false, null);
			log.info(MessageFormat.format("Publishing Message {0} to: {1} at QoS: {2}", 
					testMessage.toDebugString(), topic1, qos));
			IMqttDeliveryToken deliveryToken = pubClient.publish(topic1, testMessage);
			deliveryToken.waitForCompletion(timeout);
			
			log.info("Waiting for delivery and validating message.");
			
			ReceivedMessage receivedMsg = receiver1.getReceipt(topic1, qos, testMessage, 10);
			MqttProperties properties = receivedMsg.message.getProperties();
			assertNotNull(properties);
			Assert.assertEquals(properties1.getSubscriptionIdentifier(), properties.getSubscriptionIdentifier());
			 
			receivedMsg = receiver2.getReceipt(topic2, qos, testMessage, 10);
			properties = receivedMsg.message.getProperties();
			assertNotNull(properties);
			Assert.assertEquals(properties2.getSubscriptionIdentifier(), properties.getSubscriptionIdentifier());
			
			// Unsubscribe from the topic
			log.info("Unsubscribing from : " + topic1);
			IMqttToken unsubscribeToken = subClient1.unsubscribe(topic1);
			unsubscribeToken.waitForCompletion(timeout);
			log.info("Unsubscribing from : " + topic2);
			unsubscribeToken = subClient1.unsubscribe(topic2);
			unsubscribeToken.waitForCompletion(timeout);
		}
		
		TestClientUtilities.disconnectAndCloseClient(pubClient, 5000);
		TestClientUtilities.disconnectAndCloseClient(subClient1, 5000);
		TestClientUtilities.disconnectAndCloseClient(subClient2, 5000);
	}

}

package emq.paho.mqtt5.support.test;

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
import emq.paho.mqtt5.support.test.utils.TestClientUtilities;
import emq.paho.mqtt5.support.test.utils.Utility;

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
		MqttAsyncClient asyncClient = TestClientUtilities.connectAndGetClient(TestHelper.getServerURI().toString(), clientId,
				mqttV5Receiver, null, timeout);
		
		for (int qos = 0; qos <= 2; qos++) {
			log.info("Testing Publish and Receive at QoS: " + qos);
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
			MqttMessage testMessage = new MqttMessage(messagePayload.getBytes(), qos, false, null);
			testMessage.setProperties(properties);
			log.info(MessageFormat.format("Publishing Message {0} to: {1} at QoS: {2}", testMessage.toDebugString(), topic, qos));
			IMqttDeliveryToken deliveryToken = asyncClient.publish(topic, testMessage);
			deliveryToken.waitForCompletion(timeout);

			log.info("Waiting for delivery and validating message.");
			boolean received = mqttV5Receiver.validateReceipt(topic, qos, testMessage);
			Assert.assertTrue(received);
			
			// Publish a message to the topic using topic alias
			messagePayload = MessageFormat.format("[Topic alias] Test Payload at QoS : {0}", qos);
			testMessage = new MqttMessage(messagePayload.getBytes(), qos, false, null);
			log.info(MessageFormat.format("Publishing Message {0} to: {1} at QoS: {2} using topic alias {3}", 
					testMessage.toDebugString(), topic, qos, topicAlias));
			deliveryToken = asyncClient.publish(null, testMessage);
			deliveryToken.waitForCompletion(timeout);

			log.info("Waiting for delivery and validating message.");
			received = mqttV5Receiver.validateReceipt(topic, qos, testMessage);
			Assert.assertTrue(received);
		}
		
		TestClientUtilities.disconnectAndCloseClient(asyncClient, 5000);
	}

}

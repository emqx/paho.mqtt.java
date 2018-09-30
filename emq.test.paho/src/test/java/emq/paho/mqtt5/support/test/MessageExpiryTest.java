package emq.paho.mqtt5.support.test;

import java.text.MessageFormat;
import java.util.concurrent.TimeUnit;
import java.util.logging.Logger;

import org.eclipse.paho.mqttv5.client.IMqttDeliveryToken;
import org.eclipse.paho.mqttv5.client.IMqttToken;
import org.eclipse.paho.mqttv5.client.MqttAsyncClient;
import org.eclipse.paho.mqttv5.client.test.logging.LoggingUtilities;
import org.eclipse.paho.mqttv5.client.test.utilities.Utility;
import org.eclipse.paho.mqttv5.common.MqttException;
import org.eclipse.paho.mqttv5.common.MqttMessage;
import org.eclipse.paho.mqttv5.common.MqttSubscription;
import org.eclipse.paho.mqttv5.common.packet.MqttProperties;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import emq.paho.mqtt5.support.test.utils.EMQMqttV5Receiver;
import emq.paho.mqtt5.support.test.utils.TestClientUtilities;

public class MessageExpiryTest {
	
	private static final Logger log = Logger.getLogger(MessageExpiryTest.class.getName());

	@BeforeClass
	public static void setUpBeforeClass() throws Exception {
		TestHelper.setUpBeforeClass();
	}
	
	
	@Test
	public void testPubExpiry() throws MqttException, InterruptedException {
		String clientId = Utility.getMethodName();
		String topic = TestHelper.getTopicPrefix() + clientId;
		
		int timeout = 120 * 1000;
		
		EMQMqttV5Receiver mqttV5Receiver = new EMQMqttV5Receiver(clientId, LoggingUtilities.getPrintStream());
		MqttAsyncClient asyncClient = TestClientUtilities.connectAndGetClient(TestHelper.getServerURI().toString(), clientId,
				mqttV5Receiver, null, timeout);
		
		//test normal publication
		for (int qos = 0; qos <= 2; qos++) {
			log.info("Testing Publish and Receive at QoS: " + qos);
			// Subscribe to a topic
			log.info(MessageFormat.format("Subscribing to: {0} at QoS {1}", topic, qos));
			MqttSubscription subscription = new MqttSubscription(topic, qos);
			IMqttToken subscribeToken = asyncClient.subscribe(subscription);
			subscribeToken.waitForCompletion(timeout);

			// Publish a message to the topic
			String messagePayload = "Test Payload at QoS : " + qos;
			MqttMessage testMessage = new MqttMessage(messagePayload.getBytes(), qos, false, null);
			log.info(MessageFormat.format("Publishing Message {0} to: {1} at QoS: {2}", testMessage.toDebugString(), topic, qos));
			IMqttDeliveryToken deliveryToken = asyncClient.publish(topic, testMessage);
			deliveryToken.waitForCompletion(timeout);

			log.info("Waiting for delivery and validating message.");
			boolean received = mqttV5Receiver.validateReceipt(topic, qos, testMessage);
			Assert.assertTrue(received);

			// Unsubscribe from the topic
			log.info("Unsubscribing from : " + topic);
			IMqttToken unsubscribeToken = asyncClient.unsubscribe(topic);
			unsubscribeToken.waitForCompletion(timeout);
		}
		
		//test publication expiry
		long msgExpiryInterval = 10l;
		MqttProperties properties = new MqttProperties();
		properties.setMessageExpiryInterval(msgExpiryInterval);
		
		for (int qos = 0; qos <= 2; qos++) {
			log.info("Testing Publication Expiry at QoS: " + qos);
			// Publish a message to the topic
			String messagePayload = "Test Payload at QoS : " + qos;
			MqttMessage testMessage = new MqttMessage(messagePayload.getBytes(), qos, false, properties);
			log.info(MessageFormat.format("Publishing Message {0} to: {1} at QoS: {2} with expiry interval {3}", 
					testMessage.toDebugString(), topic, qos, msgExpiryInterval));
			IMqttDeliveryToken deliveryToken = asyncClient.publish(topic, testMessage);
			deliveryToken.waitForCompletion(timeout);
			
			//wait until the publication is expired
			TimeUnit.SECONDS.sleep(2 * msgExpiryInterval);
			
			// Subscribe to a topic
			log.info(MessageFormat.format("Subscribing to: {0} at QoS {1}", topic, qos));
			MqttSubscription subscription = new MqttSubscription(topic, qos);
			IMqttToken subscribeToken = asyncClient.subscribe(subscription);
			subscribeToken.waitForCompletion(timeout);

			log.info("Waiting for delivery and validating message.");
			boolean received = mqttV5Receiver.validateReceipt(topic, qos, testMessage, 20);
			Assert.assertFalse(received);

			// Unsubscribe from the topic
			log.info("Unsubscribing from : " + topic);
			IMqttToken unsubscribeToken = asyncClient.unsubscribe(topic);
			unsubscribeToken.waitForCompletion(timeout);
		}
	}
}

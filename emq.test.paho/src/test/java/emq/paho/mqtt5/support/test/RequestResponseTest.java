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

import emq.paho.mqtt5.support.test.utils.EMQMqttV5ConnectActionListener;
import emq.paho.mqtt5.support.test.utils.EMQMqttV5Receiver;
import emq.paho.mqtt5.support.test.utils.TestClientUtilities;
import emq.paho.mqtt5.support.test.utils.Utility;
import emq.paho.mqtt5.support.test.utils.EMQMqttV5Receiver.ReceivedMessage;

/*
 * Finished
 */
public class RequestResponseTest {

	private static final Logger log = Logger.getLogger(RequestResponseTest.class.getName());
	
	@BeforeClass
	public static void setUpBeforeClass() throws Exception {
		TestHelper.setUpBeforeClass();
	}
	
	// Server doesn't return response info in CONNACK yet
	@Test
	public void testRequestResponseInformation() throws MqttException, InterruptedException {
		String clientId = Utility.getMethodName();
		int timeout = 120 * 1000;
		
		LoggingUtilities.banner(log, RequestResponseTest.class, clientId);
		String topic = TestHelper.getTopicPrefix() + clientId;
		
		EMQMqttV5ConnectActionListener conActListener = new EMQMqttV5ConnectActionListener();
		EMQMqttV5Receiver receiver = new EMQMqttV5Receiver(clientId, LoggingUtilities.getPrintStream());
		MqttConnectionOptions connOptions = new MqttConnectionOptions();
		connOptions.setRequestResponseInfo(true);
		MqttAsyncClient asyncClient = TestClientUtilities.connectAndGetClient(TestHelper.getServerURI().toString(), clientId,
				receiver, connOptions, conActListener, timeout);
		
		TestClientUtilities.disconnectAndCloseClient(asyncClient, 5000);
	}
	
	@Test
	public void testResponseTopic() throws MqttException, InterruptedException {
		String clientId = Utility.getMethodName();
		int timeout = 120 * 1000;
		
		LoggingUtilities.banner(log, RequestResponseTest.class, clientId);
		String topic = TestHelper.getTopicPrefix() + clientId;
		
		EMQMqttV5Receiver requesterReceiver = new EMQMqttV5Receiver(clientId + "-req", LoggingUtilities.getPrintStream());
		MqttAsyncClient requester = TestClientUtilities.connectAndGetClient(TestHelper.getServerURI().toString(), clientId + "-req",
				requesterReceiver, null, timeout);
		
		EMQMqttV5Receiver responderReceiver = new EMQMqttV5Receiver(clientId + "-resp", LoggingUtilities.getPrintStream());
		MqttAsyncClient responder = TestClientUtilities.connectAndGetClient(TestHelper.getServerURI().toString(), clientId + "-resp",
				responderReceiver, null, timeout);
		
		for (int qos = 0; qos <= 2; qos++) {
			// Subscribe to the topic
			log.info(MessageFormat.format("Subscribing to topic: {0} at QoS {1}", topic, qos));
			MqttSubscription subscription = new MqttSubscription(topic, qos);
			IMqttToken subscribeToken = requester.subscribe(subscription);
			subscribeToken.waitForCompletion(timeout);
			
			// Publish a message to the topic with response topic
			String responseTopic = TestHelper.getTopicPrefix() + "respTopic";
			// Subscribe to it first
			subscription = new MqttSubscription(responseTopic, qos);
			subscribeToken = responder.subscribe(subscription);
			subscribeToken.waitForCompletion(timeout);
			// Then publish it
			String messagePayload = "Test Payload at QoS : " + qos;
			MqttMessage testMessage = new MqttMessage(messagePayload.getBytes(), qos, false, null);
			log.info(MessageFormat.format("Publishing Message {0} to: {1} at QoS: {2}", 
					testMessage.toDebugString(), topic, qos));
			MqttProperties properties = new MqttProperties();
			properties.setResponseTopic(responseTopic);
			testMessage.setProperties(properties);
			IMqttDeliveryToken deliveryToken = responder.publish(topic, testMessage);
			deliveryToken.waitForCompletion(timeout);
			
			log.info("Waiting for delivery and validating message.");
			ReceivedMessage receivedMsg = requesterReceiver.getReceipt(topic, qos, testMessage, 10);
			Assert.assertNotNull(receivedMsg);
			properties = receivedMsg.message.getProperties();
			Assert.assertNotNull(properties);
			String responseTopicReceived = properties.getResponseTopic();
			Assert.assertEquals(responseTopic, responseTopicReceived);
			// Publish to response topic
			messagePayload = "Message to response topic QoS : " + qos;
			testMessage = new MqttMessage(messagePayload.getBytes(), qos, false, null);
			deliveryToken = requester.publish(responseTopicReceived, testMessage);
			deliveryToken.waitForCompletion(timeout);
			
			boolean received = responderReceiver.validateReceipt(responseTopic, qos, testMessage, 10);
			Assert.assertTrue(received);
		}
		
		TestClientUtilities.disconnectAndCloseClient(requester, 5000);
		TestClientUtilities.disconnectAndCloseClient(responder, 5000);
	}
	
	@Test
	public void testResponseTopicWithCorrelationData() throws MqttException, InterruptedException {
		String clientId = Utility.getMethodName();
		int timeout = 120 * 1000;
		
		LoggingUtilities.banner(log, RequestResponseTest.class, clientId);
		String topic = TestHelper.getTopicPrefix() + clientId;
		
		EMQMqttV5Receiver requesterReceiver = new EMQMqttV5Receiver(clientId + "-req", LoggingUtilities.getPrintStream());
		MqttAsyncClient requester = TestClientUtilities.connectAndGetClient(TestHelper.getServerURI().toString(), clientId + "-req",
				requesterReceiver, null, timeout);
		
		EMQMqttV5Receiver responderReceiver = new EMQMqttV5Receiver(clientId + "-resp", LoggingUtilities.getPrintStream());
		MqttAsyncClient responder = TestClientUtilities.connectAndGetClient(TestHelper.getServerURI().toString(), clientId + "-resp",
				responderReceiver, null, timeout);
		
		for (int qos = 0; qos <= 2; qos++) {
			// Subscribe to the topic
			log.info(MessageFormat.format("Subscribing to topic: {0} at QoS {1}", topic, qos));
			MqttSubscription subscription = new MqttSubscription(topic, qos);
			IMqttToken subscribeToken = requester.subscribe(subscription);
			subscribeToken.waitForCompletion(timeout);
			
			// Publish a message to the topic with response topic
			String responseTopic = TestHelper.getTopicPrefix() + "respTopic";
			// Subscribe to it first
			subscription = new MqttSubscription(responseTopic, qos);
			subscribeToken = responder.subscribe(subscription);
			subscribeToken.waitForCompletion(timeout);
			// Then publish it
			String messagePayload = "Test Payload at QoS : " + qos;
			MqttMessage testMessage = new MqttMessage(messagePayload.getBytes(), qos, false, null);
			log.info(MessageFormat.format("Publishing Message {0} to: {1} at QoS: {2}", 
					testMessage.toDebugString(), topic, qos));
			MqttProperties properties = new MqttProperties();
			properties.setResponseTopic(responseTopic);
			String correlationData = "Correlation data at Qos: " + qos;
			properties.setCorrelationData(correlationData.getBytes());
			testMessage.setProperties(properties);
			IMqttDeliveryToken deliveryToken = responder.publish(topic, testMessage);
			deliveryToken.waitForCompletion(timeout);
			
			log.info("Waiting for delivery and validating message.");
			ReceivedMessage receivedMsg = requesterReceiver.getReceipt(topic, qos, testMessage, 10);
			Assert.assertNotNull(receivedMsg);
			properties = receivedMsg.message.getProperties();
			Assert.assertNotNull(properties);
			String responseTopicReceived = properties.getResponseTopic();
			Assert.assertEquals(responseTopic, responseTopicReceived);
			byte[] data = properties.getCorrelationData();
			Assert.assertEquals(correlationData, new String(data));
			// Publish to response topic
			testMessage = new MqttMessage(data, qos, false, null);
			deliveryToken = requester.publish(responseTopicReceived, testMessage);
			deliveryToken.waitForCompletion(timeout);
			
			boolean received = responderReceiver.validateReceipt(responseTopic, qos, testMessage, 10);
			Assert.assertTrue(received);
		}
		
		TestClientUtilities.disconnectAndCloseClient(requester, 5000);
		TestClientUtilities.disconnectAndCloseClient(responder, 5000);
	}
}
	

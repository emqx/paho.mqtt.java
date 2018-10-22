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
import org.eclipse.paho.mqttv5.common.packet.MqttReturnCode;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import emq.paho.mqtt5.support.test.utils.EMQMqttV5ConnectActionListener;
import emq.paho.mqtt5.support.test.utils.EMQMqttV5Receiver;
import emq.paho.mqtt5.support.test.utils.TestClientUtilities;
import emq.paho.mqtt5.support.test.utils.Utility;
import emq.paho.mqtt5.support.test.utils.EMQMqttV5Receiver.ReceivedMessage;

public class PayloadFormatTest {
	
	private static final Logger log = Logger.getLogger(PayloadFormatTest.class.getName());
	
	@BeforeClass
	public static void setUpBeforeClass() throws Exception {
		TestHelper.setUpBeforeClass();
	}
	
	@Test
	public void testPubPayloadFormat() throws MqttException, InterruptedException {
		String clientId = Utility.getMethodName();
		int timeout = 120 * 1000;
		
		LoggingUtilities.banner(log, PayloadFormatTest.class, clientId);
		String topic = TestHelper.getTopicPrefix() + clientId;
		
		EMQMqttV5ConnectActionListener subConActListener = new EMQMqttV5ConnectActionListener();
		EMQMqttV5Receiver subReceiver = new EMQMqttV5Receiver(clientId + "-sub", LoggingUtilities.getPrintStream());
		MqttAsyncClient subClient = TestClientUtilities.connectAndGetClient(TestHelper.getServerURI().toString(), clientId + "-sub",
				subReceiver, null, subConActListener, timeout);
		
		EMQMqttV5ConnectActionListener pubConActListener = new EMQMqttV5ConnectActionListener();
		EMQMqttV5Receiver pubReceiver = new EMQMqttV5Receiver(clientId + "-pub", LoggingUtilities.getPrintStream());
		MqttAsyncClient pubClient = TestClientUtilities.connectAndGetClient(TestHelper.getServerURI().toString(), clientId + "-pub",
				pubReceiver, null, pubConActListener, timeout);
		
		for (int qos = 0; qos <= 2; qos++) {
			// Subscribe to the topic
			log.info(MessageFormat.format("Subscribing to topic: {0} at QoS {1}", topic, qos));
			MqttSubscription subscription = new MqttSubscription(topic, qos);
			IMqttToken subscribeToken = subClient.subscribe(subscription);
			subscribeToken.waitForCompletion(timeout);
			
			// Publish messages with no payload format indicator
			String messagePayload = "Test Payload at QoS : " + qos;
			MqttProperties properties = new MqttProperties();
			MqttMessage testMessage = new MqttMessage(messagePayload.getBytes(), qos, false, properties);
			IMqttDeliveryToken deliveryToken = pubClient.publish(topic, testMessage);
			deliveryToken.waitForCompletion(timeout);
			
			log.info("Waiting for delivery and validating message.");
			ReceivedMessage received = subReceiver.getReceipt(topic, qos, testMessage, 10);
			properties = received.message.getProperties();
			Assert.assertNotNull(properties);
			Assert.assertEquals(false, properties.getPayloadFormat());
			
			// Publish messages
			messagePayload = "Test Payload at QoS : " + qos;
			properties = new MqttProperties();
			properties.setPayloadFormat(true);
			testMessage = new MqttMessage(messagePayload.getBytes(), qos, false, properties);
			deliveryToken = pubClient.publish(topic, testMessage);
			deliveryToken.waitForCompletion(timeout);
			
			log.info("Waiting for delivery and validating message.");
			received = subReceiver.getReceipt(topic, qos, testMessage, 10);
			properties = received.message.getProperties();
			Assert.assertNotNull(properties);
			Assert.assertEquals(true, properties.getPayloadFormat());
		}
		
		TestClientUtilities.disconnectAndCloseClient(subClient, 5000);
		TestClientUtilities.disconnectAndCloseClient(pubClient, 5000);
	}
	
	@Test
	public void testWillPayloadFormat() throws MqttException, InterruptedException {
		String clientId = Utility.getMethodName();
		int timeout = 120 * 1000;
		
		LoggingUtilities.banner(log, ContentTypeTest.class, clientId);
		String topic = TestHelper.getTopicPrefix() + clientId;
		
		for (int qos = 0; qos <= 2; qos++) {
			EMQMqttV5ConnectActionListener subConActListener = new EMQMqttV5ConnectActionListener();
			EMQMqttV5Receiver subReceiver = new EMQMqttV5Receiver(clientId + "-sub", LoggingUtilities.getPrintStream());
			MqttAsyncClient subClient = TestClientUtilities.connectAndGetClient(TestHelper.getServerURI().toString(), clientId + "-sub",
					subReceiver, null, subConActListener, timeout);
			
			// Subscribe to the topic
			log.info(MessageFormat.format("Subscribing to topic: {0} at QoS {1}", topic, qos));
			MqttSubscription subscription = new MqttSubscription(topic, qos);
			IMqttToken subscribeToken = subClient.subscribe(subscription);
			subscribeToken.waitForCompletion(timeout);
			
			// Will message with no payload format indicator
			EMQMqttV5ConnectActionListener pubConActListener = new EMQMqttV5ConnectActionListener();
			MqttConnectionOptions connOptions = new MqttConnectionOptions();
			MqttMessage willMessage = new MqttMessage(new String("Will from " + clientId).getBytes(), qos, false, null);
			connOptions.setWill(topic, willMessage);
			MqttProperties willProperties = new MqttProperties();
			willProperties.setPayloadFormat(false);
			connOptions.setWillMessageProperties(willProperties);
			MqttAsyncClient pubClient = TestClientUtilities.connectAndGetClient(TestHelper.getServerURI().toString(), clientId + "-pub",
					null, connOptions, pubConActListener, timeout);
			
			// Disconnect the client with reason code not equals to 0
			log.info("Disconnecting client: [" + pubClient.getClientId() + "]");
			IMqttToken disconnectToken = pubClient.disconnect(30000, null, null, MqttReturnCode.RETURN_CODE_SERVER_BUSY,
					new MqttProperties());
			disconnectToken.waitForCompletion(timeout);
			Assert.assertFalse(pubClient.isConnected());
			pubClient.close();
			log.info("Client [" + pubClient.getClientId() + "] disconnected and closed.");
			
			log.info("Waiting for will message.");
			// Will message arrives immediately
			ReceivedMessage received = subReceiver.getReceipt(topic, qos, willMessage, 10);
			MqttProperties properties = received.message.getProperties();
			Assert.assertNotNull(properties);
			Assert.assertEquals(false, properties.getPayloadFormat());
			
			// Will message with payload format indicator
			pubConActListener = new EMQMqttV5ConnectActionListener();
			connOptions = new MqttConnectionOptions();
			willMessage = new MqttMessage(new String("Will from " + clientId).getBytes(), qos, false, null);
			connOptions.setWill(topic, willMessage);
			willProperties = new MqttProperties();
			willProperties.setPayloadFormat(true);
			connOptions.setWillMessageProperties(willProperties);
			pubClient = TestClientUtilities.connectAndGetClient(TestHelper.getServerURI().toString(), clientId + "-pub",
					null, connOptions, pubConActListener, timeout);
			
			// Disconnect the client with reason code not equals to 0
			log.info("Disconnecting client: [" + pubClient.getClientId() + "]");
			disconnectToken = pubClient.disconnect(30000, null, null, MqttReturnCode.RETURN_CODE_SERVER_BUSY,
					new MqttProperties());
			disconnectToken.waitForCompletion(timeout);
			Assert.assertFalse(pubClient.isConnected());
			pubClient.close();
			log.info("Client [" + pubClient.getClientId() + "] disconnected and closed.");
			
			log.info("Waiting for will message.");
			// Will message arrives immediately
			received = subReceiver.getReceipt(topic, qos, willMessage, 10);
			properties = received.message.getProperties();
			Assert.assertNotNull(properties);
			Assert.assertEquals(true, properties.getPayloadFormat());
			
			// Unsubscribe from the topic
			log.info("Unsubscribing from : " + topic);
			IMqttToken unsubscribeToken = subClient.unsubscribe(topic);
			unsubscribeToken.waitForCompletion(timeout);
			
			TestClientUtilities.disconnectAndCloseClient(subClient, 5000);
		}
	}
	
	

}

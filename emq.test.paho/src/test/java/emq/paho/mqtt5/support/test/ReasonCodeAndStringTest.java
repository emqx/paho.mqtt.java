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
import org.eclipse.paho.mqttv5.common.packet.MqttReturnCode;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import emq.paho.mqtt5.support.test.utils.EMQMqttV5Receiver;
import emq.paho.mqtt5.support.test.utils.TestClientUtilities;
import emq.paho.mqtt5.support.test.utils.Utility;

/*
 * Finished
 */
public class ReasonCodeAndStringTest {
	
private static final Logger log = Logger.getLogger(TopicAliasTest.class.getName());
	
	@BeforeClass
	public static void setUpBeforeClass() throws Exception {
		TestHelper.setUpBeforeClass();
	}
	
	@Test
	public void testConnAckReasonCode() throws MqttException, InterruptedException {
		String clientId = Utility.getMethodName();
		int timeout = 120 * 1000;
		
		LoggingUtilities.banner(log, TopicAliasTest.class, clientId);
		
		String serverURI = TestHelper.getServerURI().toString();
		MqttAsyncClient asyncClient = new MqttAsyncClient(serverURI, clientId, null);
		log.info("Connecting: [serverURI: " + serverURI + ", ClientId: " + clientId + "]");
		IMqttToken connectToken = asyncClient.connect();
		connectToken.waitForCompletion(timeout);
		Assert.assertTrue(asyncClient.isConnected());
		int[] responseCodes = connectToken.getResponse().getReasonCodes();
		Assert.assertNotNull(responseCodes);
		Assert.assertEquals(1, responseCodes.length);
		Assert.assertEquals(0, responseCodes[0]);
		
		// Disconnect
		log.info("Disconnecting client: [" + asyncClient.getClientId() + "]");
		IMqttToken disconnectToken = asyncClient.disconnect();
		disconnectToken.waitForCompletion(timeout);
		responseCodes = disconnectToken.getReasonCodes();
		Assert.assertFalse(asyncClient.isConnected());
		asyncClient.close();
		Assert.assertNotNull(responseCodes);
		Assert.assertEquals(1, responseCodes.length);
		Assert.assertEquals(0, responseCodes[0]);
		log.info("Client [" + asyncClient.getClientId() + "] disconnected and closed.");
		
		// Disconnect with will message
		asyncClient = TestClientUtilities.connectAndGetClient(TestHelper.getServerURI().toString(), clientId,
				null, null, timeout);
		log.info("Disconnecting client: [" + asyncClient.getClientId() + "]");
		disconnectToken = asyncClient.disconnect(30000, null, null, MqttReturnCode.RETURN_CODE_DISCONNECT_WITH_WILL_MESSAGE, null);
		disconnectToken.waitForCompletion(timeout);
		responseCodes = disconnectToken.getReasonCodes();
		Assert.assertFalse(asyncClient.isConnected());
		asyncClient.close();
		Assert.assertNotNull(responseCodes);
		Assert.assertEquals(1, responseCodes.length);
		Assert.assertEquals(MqttReturnCode.RETURN_CODE_DISCONNECT_WITH_WILL_MESSAGE, responseCodes[0]);
		log.info("Client [" + asyncClient.getClientId() + "] disconnected and closed.");
	}
	
	@Test
	public void testPubAckReasonCode() throws MqttException, InterruptedException {
		String clientId = Utility.getMethodName();
		int timeout = 120 * 1000;
		
		LoggingUtilities.banner(log, TopicAliasTest.class, clientId);
		String topic = TestHelper.getTopicPrefix() + clientId;
		
		EMQMqttV5Receiver mqttV5Receiver = new EMQMqttV5Receiver(clientId, LoggingUtilities.getPrintStream());
		MqttConnectionOptions connOptions = new MqttConnectionOptions();
		connOptions.setTopicAliasMaximum(10);
		MqttAsyncClient asyncClient = TestClientUtilities.connectAndGetClient(TestHelper.getServerURI().toString(), clientId,
				mqttV5Receiver, connOptions, timeout);
		
		for (int qos = 1; qos <= 2; qos++) {
			// Publish a message
			String messagePayload = "Test Payload at QoS : " + qos;
			MqttMessage testMessage = new MqttMessage(messagePayload.getBytes(), qos, false, null);
			log.info(MessageFormat.format("Publishing Message {0} to: {1} at QoS: {2}", 
					testMessage.toDebugString(), topic, qos));
			IMqttDeliveryToken deliveryToken = asyncClient.publish(topic, testMessage);
			deliveryToken.waitForCompletion(timeout);
			int[] responseCodes = deliveryToken.getReasonCodes();
			Assert.assertNotNull(responseCodes);
			Assert.assertNotEquals(0, responseCodes.length);
			Assert.assertEquals(16, responseCodes[0]);
			
			// Publish at an invalid topic (paho client will stop that)
			String invalidTopic = "topic/+";
			log.info(MessageFormat.format("Publishing Message {0} to: {1} at QoS: {2}", 
					testMessage.toDebugString(), invalidTopic, qos));
			try {
				deliveryToken = asyncClient.publish(invalidTopic, testMessage);
				Assert.fail();
			} catch (Exception e) {
				
			}
//			deliveryToken.waitForCompletion(timeout);
//			responseCodes = deliveryToken.getReasonCodes();
//			Assert.assertNotNull(responseCodes);
//			Assert.assertNotEquals(0, responseCodes.length);
//			Assert.assertEquals(144, responseCodes[0]);
			
			// Subscribe the topic then pub
			log.info(MessageFormat.format("Subscribing to topic: {0} at QoS {1}", topic, qos));
			MqttSubscription subscription = new MqttSubscription(topic, qos);
			IMqttToken subscribeToken = asyncClient.subscribe(subscription);
			subscribeToken.waitForCompletion(timeout);
			
			deliveryToken = asyncClient.publish(topic, testMessage);
			deliveryToken.waitForCompletion(timeout);
			responseCodes = deliveryToken.getReasonCodes();
			Assert.assertNotNull(responseCodes);
			Assert.assertNotEquals(0, responseCodes.length);
			Assert.assertEquals(0, responseCodes[0]);
			// Unsubscribe
			log.info("Unsubscribing from : " + topic);
			IMqttToken unsubscribeToken = asyncClient.unsubscribe(topic);
			unsubscribeToken.waitForCompletion(timeout);
			
		}
		
		TestClientUtilities.disconnectAndCloseClient(asyncClient, 5000);
	}
	
	@Test
	public void testSubAckReasonCode() throws MqttException, InterruptedException {
		String clientId = Utility.getMethodName();
		int timeout = 120 * 1000;
		
		LoggingUtilities.banner(log, TopicAliasTest.class, clientId);
		String topic = TestHelper.getTopicPrefix() + clientId;
		
		EMQMqttV5Receiver mqttV5Receiver = new EMQMqttV5Receiver(clientId, LoggingUtilities.getPrintStream());
		MqttConnectionOptions connOptions = new MqttConnectionOptions();
		connOptions.setTopicAliasMaximum(10);
		MqttAsyncClient asyncClient = TestClientUtilities.connectAndGetClient(TestHelper.getServerURI().toString(), clientId,
				mqttV5Receiver, connOptions, timeout);
		
		for (int qos = 1; qos <= 2; qos++) {
			// Subscribe to a topic
			log.info(MessageFormat.format("Subscribing to topic: {0} at QoS {1}", topic, qos));
			MqttSubscription subscription = new MqttSubscription(topic, qos);
			IMqttToken subscribeToken = asyncClient.subscribe(subscription);
			subscribeToken.waitForCompletion(timeout);
			int[] responseCodes = subscribeToken.getResponse().getReasonCodes();
			Assert.assertNotNull(responseCodes);
			Assert.assertNotEquals(0, responseCodes.length);
			Assert.assertEquals(qos, responseCodes[0]);
			
			// Unsubscribe
			log.info("Unsubscribing from : " + topic);
			IMqttToken unsubscribeToken = asyncClient.unsubscribe(topic);
			unsubscribeToken.waitForCompletion(timeout);
			responseCodes = unsubscribeToken.getResponse().getReasonCodes();
			Assert.assertNotNull(responseCodes);
			Assert.assertNotEquals(0, responseCodes.length);
		}
		
		TestClientUtilities.disconnectAndCloseClient(asyncClient, 5000);
	}

}

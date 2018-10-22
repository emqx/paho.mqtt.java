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
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import emq.paho.mqtt5.support.test.utils.EMQMqttV5Receiver;
import emq.paho.mqtt5.support.test.utils.TestClientUtilities;
import emq.paho.mqtt5.support.test.utils.Utility;

public class MaximumPacketSizeTest {
	
	private static final Logger log = Logger.getLogger(MaximumPacketSizeTest.class.getName());
	
	@BeforeClass
	public static void setUpBeforeClass() throws Exception {
		TestHelper.setUpBeforeClass();
	}
	
	@Test
	public void testClientMaxPacketSize() throws MqttException, InterruptedException {
		String clientId = Utility.getMethodName();
		int timeout = 120 * 1000;
		
		LoggingUtilities.banner(log, SubscriptionOptionsTest.class, clientId);
		String topic = TestHelper.getTopicPrefix() + clientId;
		
		
		MqttAsyncClient pubClient = TestClientUtilities.connectAndGetClient(TestHelper.getServerURI().toString(), clientId + "-pub",
				null, null, timeout);
		
		EMQMqttV5Receiver receiver = new EMQMqttV5Receiver(clientId, LoggingUtilities.getPrintStream());
		MqttConnectionOptions connOptions = new MqttConnectionOptions();
		connOptions.setMaximumPacketSize(10l);
		MqttAsyncClient subClient = TestClientUtilities.connectAndGetClient(TestHelper.getServerURI().toString(), clientId + "-sub",
				receiver, connOptions, timeout);
		
		for (int qos = 1; qos <= 2; qos++) {
			// Subscribe to a topic
			log.info(MessageFormat.format("Subscribing to topic: {0} at QoS {1}", topic, qos));
			MqttSubscription subscription = new MqttSubscription(topic, qos);
			IMqttToken subscribeToken = subClient.subscribe(subscription);
			subscribeToken.waitForCompletion(timeout);
			
			// Publish a message to the topic
			String messagePayload = "Test Payload at QoS : " + qos;
			for (int i=0; i<100; i++) {
				messagePayload += "!";
			}
			System.out.println(messagePayload.getBytes().length);
			MqttMessage testMessage = new MqttMessage(messagePayload.getBytes(), qos, false, null);
			log.info(MessageFormat.format("Publishing Message {0} to: {1} at QoS: {2}", 
					testMessage.toDebugString(), topic, qos));
			IMqttDeliveryToken deliveryToken = pubClient.publish(topic, testMessage);
			deliveryToken.waitForCompletion(timeout);
			
			receiver.getReceipt(topic, qos, testMessage, 10);
			Assert.assertFalse(receiver.isConnected());
		}
		Thread.sleep(10000);
//		TestClientUtilities.disconnectAndCloseClient(pubClient, 5000);
//		TestClientUtilities.disconnectAndCloseClient(subClient, 5000);
	}

}

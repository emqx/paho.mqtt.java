package emq.paho.mqtt5.support.test;

import java.text.MessageFormat;
import java.util.logging.Logger;

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

import emq.paho.mqtt5.support.test.utils.EMQMqttV5Receiver;
import emq.paho.mqtt5.support.test.utils.TestClientUtilities;
import emq.paho.mqtt5.support.test.utils.Utility;

/*
 * Finished
 */
public class WillDelayTest {
	
	private static final Logger log = Logger.getLogger(WillDelayTest.class.getName());
	
	@BeforeClass
	public static void setUpBeforeClass() throws Exception {
		TestHelper.setUpBeforeClass();
	}
	
	@Test
	public void testNoWillDelay() throws MqttException, InterruptedException {
		String clientId = Utility.getMethodName();
		int timeout = 120 * 1000;
		
		LoggingUtilities.banner(log, WillDelayTest.class, clientId);
		String topic = TestHelper.getTopicPrefix() + clientId;
		
		EMQMqttV5Receiver receiver = new EMQMqttV5Receiver(clientId, LoggingUtilities.getPrintStream());
		MqttConnectionOptions connOptions = new MqttConnectionOptions();
		MqttMessage willMessage = new MqttMessage(new String("Will from " + clientId).getBytes(), 0, false, null);
		connOptions.setWill(topic, willMessage);
		MqttProperties willProperties = new MqttProperties();
		connOptions.setWillMessageProperties(willProperties);
		MqttAsyncClient asyncClient = TestClientUtilities.connectAndGetClient(TestHelper.getServerURI().toString(), clientId,
				receiver, connOptions, timeout);
		
		EMQMqttV5Receiver subReceiver = new EMQMqttV5Receiver(clientId, LoggingUtilities.getPrintStream());
		MqttAsyncClient subClient = TestClientUtilities.connectAndGetClient(TestHelper.getServerURI().toString(), clientId + "-sub",
				subReceiver, null, timeout);
		
		// Subscribe to the topic
		int qos = 0;
		log.info(MessageFormat.format("Subscribing to topic: {0} at QoS {1}", topic, qos));
		MqttSubscription subscription = new MqttSubscription(topic, 0);
		subscription.setNoLocal(false);
		IMqttToken subscribeToken = subClient.subscribe(subscription);
		subscribeToken.waitForCompletion(timeout);
		
		// Disconnect the client with reason code not equals to 0
		log.info("Disconnecting client: [" + asyncClient.getClientId() + "]");
		IMqttToken disconnectToken = asyncClient.disconnect(30000, null, null, MqttReturnCode.RETURN_CODE_SERVER_BUSY,
				new MqttProperties());
		disconnectToken.waitForCompletion(timeout);
		Assert.assertFalse(asyncClient.isConnected());
		asyncClient.close();
		log.info("Client [" + asyncClient.getClientId() + "] disconnected and closed.");
		
		log.info("Waiting for will message.");
		// Will message arrives immediately
		boolean received = subReceiver.validateReceipt(topic, qos, willMessage, 5);
		Assert.assertTrue(received);
		
		// Unsubscribe from the topic
		log.info("Unsubscribing from : " + topic);
		IMqttToken unsubscribeToken = subClient.unsubscribe(topic);
		unsubscribeToken.waitForCompletion(timeout);
		
		TestClientUtilities.disconnectAndCloseClient(subClient, 5000);
	}
	
	@Test
	public void testWillDelay() throws MqttException, InterruptedException {
		String clientId = Utility.getMethodName();
		int timeout = 120 * 1000;
		
		LoggingUtilities.banner(log, WillDelayTest.class, clientId);
		String topic = TestHelper.getTopicPrefix() + clientId;
		
		int willDelayInterval = 20;
		
		EMQMqttV5Receiver receiver = new EMQMqttV5Receiver(clientId, LoggingUtilities.getPrintStream());
		MqttConnectionOptions connOptions = new MqttConnectionOptions();
		MqttMessage willMessage = new MqttMessage(new String("Will from " + clientId).getBytes(), 0, false, null);
		connOptions.setWill(topic, willMessage);
		MqttProperties willProperties = new MqttProperties();
		willProperties.setWillDelayInterval((long) willDelayInterval);
		connOptions.setWillMessageProperties(willProperties);
		MqttAsyncClient asyncClient = TestClientUtilities.connectAndGetClient(TestHelper.getServerURI().toString(), clientId,
				receiver, connOptions, timeout);
		
		EMQMqttV5Receiver subReceiver = new EMQMqttV5Receiver(clientId, LoggingUtilities.getPrintStream());
		MqttAsyncClient subClient = TestClientUtilities.connectAndGetClient(TestHelper.getServerURI().toString(), clientId + "-sub",
				subReceiver, null, timeout);
		
		// Subscribe to the topic
		int qos = 0;
		log.info(MessageFormat.format("Subscribing to topic: {0} at QoS {1}", topic, qos));
		MqttSubscription subscription = new MqttSubscription(topic, 0);
		subscription.setNoLocal(false);
		IMqttToken subscribeToken = subClient.subscribe(subscription);
		subscribeToken.waitForCompletion(timeout);
		
		// Disconnect the client with reason code not equals to 0
		log.info("Disconnecting client: [" + asyncClient.getClientId() + "]");
		IMqttToken disconnectToken = asyncClient.disconnect(30000, null, null, MqttReturnCode.RETURN_CODE_SERVER_BUSY,
				new MqttProperties());
		long curTime = System.currentTimeMillis();
		disconnectToken.waitForCompletion(timeout);
		Assert.assertFalse(asyncClient.isConnected());
		asyncClient.close();
		log.info("Client [" + asyncClient.getClientId() + "] disconnected and closed.");
		
		log.info("Waiting for will message.");
		// Will message arrives after will delay interval expires
		int elapsed = (int) (System.currentTimeMillis() - curTime)/1000 + 1;
		boolean received = subReceiver.validateReceipt(topic, qos, willMessage, willDelayInterval - elapsed);
		Assert.assertFalse(received);
		received = subReceiver.validateReceipt(topic, qos, willMessage, 10);
		Assert.assertTrue(received);
		
		// Unsubscribe from the topic
		log.info("Unsubscribing from : " + topic);
		IMqttToken unsubscribeToken = subClient.unsubscribe(topic);
		unsubscribeToken.waitForCompletion(timeout);
		
		TestClientUtilities.disconnectAndCloseClient(subClient, 5000);
	}
	
	/*
	 * Not implemented yet
	 */
	@Test
	public void testWillDelayWhenRedisconnect() throws MqttException, InterruptedException {
		String clientId = Utility.getMethodName();
		int timeout = 120 * 1000;
		
		LoggingUtilities.banner(log, WillDelayTest.class, clientId);
		String topic = TestHelper.getTopicPrefix() + clientId;
		
		int willDelayInterval = 20;
		long sessionExpiryInterval = 20l;
		
		MqttConnectionOptions connOptions = new MqttConnectionOptions();
		MqttMessage willMessage = new MqttMessage(new String("Will from " + clientId).getBytes(), 0, false, null);
		connOptions.setWill(topic, willMessage);
		MqttProperties willProperties = new MqttProperties();
		willProperties.setWillDelayInterval((long) willDelayInterval);
		connOptions.setWillMessageProperties(willProperties);
		connOptions.setSessionExpiryInterval(sessionExpiryInterval);
		MqttAsyncClient asyncClient = TestClientUtilities.connectAndGetClient(TestHelper.getServerURI().toString(), clientId,
				null, connOptions, timeout);
		
		EMQMqttV5Receiver subReceiver = new EMQMqttV5Receiver(clientId, LoggingUtilities.getPrintStream());
		MqttAsyncClient subClient = TestClientUtilities.connectAndGetClient(TestHelper.getServerURI().toString(), clientId + "-sub",
				subReceiver, null, timeout);
		
		// Subscribe to the topic
		int qos = 0;
		log.info(MessageFormat.format("Subscribing to topic: {0} at QoS {1}", topic, qos));
		MqttSubscription subscription = new MqttSubscription(topic, 0);
		subscription.setNoLocal(false);
		IMqttToken subscribeToken = subClient.subscribe(subscription);
		subscribeToken.waitForCompletion(timeout);
		
		// Disconnect the client with reason code not equals to 0
		log.info("Disconnecting client: [" + asyncClient.getClientId() + "]");
		IMqttToken disconnectToken = asyncClient.disconnect(30000, null, null, MqttReturnCode.RETURN_CODE_SERVER_BUSY,
				new MqttProperties());
		disconnectToken.waitForCompletion(timeout);
		Assert.assertFalse(asyncClient.isConnected());
		asyncClient.close();
		log.info("Client [" + asyncClient.getClientId() + "] disconnected and closed.");
		
		// Reconnect
		connOptions = new MqttConnectionOptions();
		connOptions.setCleanStart(false);
		asyncClient = TestClientUtilities.connectAndGetClient(TestHelper.getServerURI().toString(), clientId,
				null, connOptions, timeout);
		
		log.info("Waiting for will message.");
		boolean received = subReceiver.validateReceipt(topic, qos, willMessage, willDelayInterval + 10);
		Assert.assertFalse(received);
		
		// Unsubscribe from the topic
		log.info("Unsubscribing from : " + topic);
		IMqttToken unsubscribeToken = subClient.unsubscribe(topic);
		unsubscribeToken.waitForCompletion(timeout);
		
		TestClientUtilities.disconnectAndCloseClient(subClient, 5000);
		TestClientUtilities.disconnectAndCloseClient(asyncClient, 5000);
	}

}

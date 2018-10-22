package emq.paho.mqtt5.support.test;

import java.util.concurrent.TimeUnit;
import java.util.logging.Logger;

import org.eclipse.paho.mqttv5.client.MqttAsyncClient;
import org.eclipse.paho.mqttv5.client.MqttConnectionOptions;
import org.eclipse.paho.mqttv5.client.test.logging.LoggingUtilities;
import org.eclipse.paho.mqttv5.common.MqttException;
import org.eclipse.paho.mqttv5.common.packet.MqttConnAck;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import emq.paho.mqtt5.support.test.utils.EMQMqttV5ConnectActionListener;
import emq.paho.mqtt5.support.test.utils.TestClientUtilities;
import emq.paho.mqtt5.support.test.utils.Utility;

/*
 * Finished
 */
public class SessionExpiryTest {
	
	private static final Logger log = Logger.getLogger(SessionExpiryTest.class.getName());
	
	@BeforeClass
	public static void setUpBeforeClass() throws Exception {
		TestHelper.setUpBeforeClass();
	}
	
	@Test
	public void testAbsentSessionExpiry() throws MqttException, InterruptedException {
		String clientId = Utility.getMethodName();
		int timeout = 120 * 1000;
		
		LoggingUtilities.banner(log, SessionExpiryTest.class, clientId);
		
		// absent session expiry interval
		MqttConnectionOptions connOptions = new MqttConnectionOptions();
		connOptions.setCleanStart(true);
		MqttAsyncClient client1 = TestClientUtilities.connectAndGetClient(TestHelper.getServerURI().toString(), clientId,
				null, connOptions, timeout);
		
		TestClientUtilities.disconnectAndCloseClient(client1, 5000);
		
		// connect using the same client Id
		connOptions = new MqttConnectionOptions();
		connOptions.setCleanStart(false);
		EMQMqttV5ConnectActionListener conActListener = new EMQMqttV5ConnectActionListener();
		MqttAsyncClient client2 = TestClientUtilities.connectAndGetClient(TestHelper.getServerURI().toString(), clientId,
				null, connOptions, conActListener, timeout);
		
		MqttConnAck connAck = conActListener.getConnAck();
		Assert.assertNull(connAck.getProperties().getSessionExpiryInterval());
		Assert.assertFalse(connAck.getSessionPresent());
		
		TestClientUtilities.disconnectAndCloseClient(client2, 5000);
	}
	
	@Test
	public void testSessionExpiry() throws MqttException, InterruptedException {
		String clientId = Utility.getMethodName();
		int timeout = 120 * 1000;
		
		LoggingUtilities.banner(log, SessionExpiryTest.class, clientId);
		
		// session expiry interval = 0
		MqttConnectionOptions connOptions = new MqttConnectionOptions();
		connOptions.setCleanStart(true);
		connOptions.setSessionExpiryInterval(0l);
		MqttAsyncClient asyncClient = TestClientUtilities.connectAndGetClient(TestHelper.getServerURI().toString(), clientId,
				null, connOptions, timeout);
		
		TestClientUtilities.disconnectAndCloseClient(asyncClient, 5000);
		
		// connect using the same client Id
		connOptions = new MqttConnectionOptions();
		connOptions.setCleanStart(false);
		EMQMqttV5ConnectActionListener conActListener = new EMQMqttV5ConnectActionListener();
		asyncClient = TestClientUtilities.connectAndGetClient(TestHelper.getServerURI().toString(), clientId,
				null, connOptions, conActListener, timeout);
		
		MqttConnAck connAck = conActListener.getConnAck();
		Assert.assertNull(connAck.getProperties().getSessionExpiryInterval());
		Assert.assertFalse(connAck.getSessionPresent());
		
		TestClientUtilities.disconnectAndCloseClient(asyncClient, 5000);
		
		////////
		// session expiry interval != 0
		long expiry = 20l;	//seconds
		connOptions = new MqttConnectionOptions();
		connOptions.setCleanStart(true);
		connOptions.setSessionExpiryInterval(expiry);
		asyncClient = TestClientUtilities.connectAndGetClient(TestHelper.getServerURI().toString(), clientId,
				null, connOptions, timeout);
		
		TestClientUtilities.disconnectAndCloseClient(asyncClient, 5000);
		
		// connect using the same client Id
		connOptions = new MqttConnectionOptions();
		connOptions.setCleanStart(false);
		conActListener = new EMQMqttV5ConnectActionListener();
		asyncClient = TestClientUtilities.connectAndGetClient(TestHelper.getServerURI().toString(), clientId,
				null, connOptions, conActListener, timeout);
		
		connAck = conActListener.getConnAck();
		Assert.assertTrue(connAck.getSessionPresent());
		
		// connect using the same client Id
		connOptions = new MqttConnectionOptions();
		connOptions.setCleanStart(false);
		conActListener = new EMQMqttV5ConnectActionListener();
		asyncClient = TestClientUtilities.connectAndGetClient(TestHelper.getServerURI().toString(), clientId,
				null, connOptions, conActListener, timeout);
		
		connAck = conActListener.getConnAck();
		Assert.assertNull(connAck.getProperties().getSessionExpiryInterval());
		Assert.assertTrue(connAck.getSessionPresent());
		
		TestClientUtilities.disconnectAndCloseClient(asyncClient, 5000);
		
		// connect after session expires
		TimeUnit.SECONDS.sleep(expiry);
		connOptions = new MqttConnectionOptions();
		connOptions.setCleanStart(false);
		conActListener = new EMQMqttV5ConnectActionListener();
		asyncClient = TestClientUtilities.connectAndGetClient(TestHelper.getServerURI().toString(), clientId,
				null, connOptions, conActListener, timeout);
		
		connAck = conActListener.getConnAck();
		Assert.assertNull(connAck.getProperties().getSessionExpiryInterval());
		Assert.assertFalse(connAck.getSessionPresent());
		
		TestClientUtilities.disconnectAndCloseClient(asyncClient, 5000);
		
	}
}

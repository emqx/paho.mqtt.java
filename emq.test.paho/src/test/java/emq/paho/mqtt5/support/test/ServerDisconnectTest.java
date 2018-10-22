package emq.paho.mqtt5.support.test;

import java.util.logging.Logger;

import org.eclipse.paho.mqttv5.client.MqttAsyncClient;
import org.eclipse.paho.mqttv5.client.test.logging.LoggingUtilities;
import org.eclipse.paho.mqttv5.common.MqttException;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import emq.paho.mqtt5.support.test.utils.TestClientUtilities;
import emq.paho.mqtt5.support.test.utils.Utility;

/*
 * More conditions to be tested
 */
public class ServerDisconnectTest {
	
	private static final Logger log = Logger.getLogger(ServerDisconnectTest.class.getName());
	
	@BeforeClass
	public static void setUpBeforeClass() throws Exception {
		TestHelper.setUpBeforeClass();
	}
	
	@Test
	public void testDisconnectForSessionOvertaken() throws MqttException, InterruptedException {
		String clientId = Utility.getMethodName();
		int timeout = 120 * 1000;
		
		LoggingUtilities.banner(log, WillDelayTest.class, clientId);
		
		MqttAsyncClient client1 = TestClientUtilities.connectAndGetClient(TestHelper.getServerURI().toString(), clientId,
				null, null, timeout);
		
		MqttAsyncClient client2 = TestClientUtilities.connectAndGetClient(TestHelper.getServerURI().toString(), clientId,
				null, null, timeout);
		
		Assert.assertFalse(client1.isConnected());
		
		TestClientUtilities.disconnectAndCloseClient(client2, 5000);
	}

}

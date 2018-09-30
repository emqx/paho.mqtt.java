package emq.paho.mqtt5.support.test;

import java.util.logging.Logger;

import org.eclipse.paho.mqttv5.client.MqttAsyncClient;
import org.eclipse.paho.mqttv5.common.MqttException;
import org.junit.BeforeClass;
import org.junit.Test;

import emq.paho.mqtt5.support.test.utils.TestClientUtilities;

/*
 * Finished
 */
public class AssignedClientIdTest {
	
	private static final Logger log = Logger.getLogger(AssignedClientIdTest.class.getName());
	
	@BeforeClass
	public static void setUpBeforeClass() throws Exception {
		TestHelper.setUpBeforeClass();
	}
	
	@Test
	public void testAssignedClientId() throws MqttException {
		String clientId = "";
		int timeout = 120 * 1000;
		
		MqttAsyncClient asyncClient = TestClientUtilities.connectAndGetClient(TestHelper.getServerURI().toString(), clientId,
				null, null, timeout);
		TestClientUtilities.disconnectAndCloseClient(asyncClient, 5000);
	}
}

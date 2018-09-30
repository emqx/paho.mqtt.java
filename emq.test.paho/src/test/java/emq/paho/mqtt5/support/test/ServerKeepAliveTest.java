package emq.paho.mqtt5.support.test;

import java.util.logging.Logger;

import org.eclipse.paho.mqttv5.client.MqttAsyncClient;
import org.eclipse.paho.mqttv5.client.MqttConnectionOptions;
import org.eclipse.paho.mqttv5.client.test.logging.LoggingUtilities;
import org.eclipse.paho.mqttv5.client.test.properties.TestProperties;
import org.eclipse.paho.mqttv5.common.MqttException;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import emq.paho.mqtt5.support.test.utils.ConnPropertiesUtil;
import emq.paho.mqtt5.support.test.utils.EMQMqttV5Receiver;
import emq.paho.mqtt5.support.test.utils.TestClientUtilities;
import emq.paho.mqtt5.support.test.utils.Utility;

/*
 * Finished
 */
public class ServerKeepAliveTest {
	
	private static final Logger log = Logger.getLogger(ServerKeepAliveTest.class.getName());
	
	@BeforeClass
	public static void setUpBeforeClass() throws Exception {
		TestHelper.setUpBeforeClass();
	}
	
	@Test
	public void testServerKeepAlive() throws MqttException, InterruptedException {
		String clientId = Utility.getMethodName();
		
		int timeout = 120 * 1000;
		int clientKeepAlive = 10;
		
		final int serverKeepAlive = Integer.valueOf(TestProperties.getInstance().getProperty("SERVER_KEEP_ALIVE"));
		
		MqttConnectionOptions connOptions = new MqttConnectionOptions();
		connOptions.setKeepAliveInterval(clientKeepAlive);
		
		EMQMqttV5Receiver mqttV5Receiver = new EMQMqttV5Receiver(clientId, LoggingUtilities.getPrintStream());
		MqttAsyncClient asyncClient = TestClientUtilities.connectAndGetClient(TestHelper.getServerURI().toString(), clientId,
				mqttV5Receiver, connOptions, timeout);
		try {
			long keepAlive = ConnPropertiesUtil.getKeepAlive(asyncClient);
			Assert.assertEquals(serverKeepAlive * 1000, keepAlive);
		} catch (Exception e) {
			e.printStackTrace();
			Assert.fail();
		}
		
		TestClientUtilities.disconnectAndCloseClient(asyncClient, 5000);
	}

}

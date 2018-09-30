package emq.paho.mqtt5.support.test;

import java.net.URI;
import java.util.UUID;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.eclipse.paho.mqttv5.client.test.SubscribeTests;
import org.eclipse.paho.mqttv5.client.test.logging.LoggingUtilities;
import org.eclipse.paho.mqttv5.client.test.utilities.Utility;
import org.junit.BeforeClass;

import emq.paho.mqtt5.support.test.utils.TestProperties;

public class TestHelper {
	
	private static Logger log = Logger.getLogger(TestHelper.class.getName());
	
	private static URI serverURI;
	private static String topicPrefix;
	
	@BeforeClass
	public static void setUpBeforeClass() throws Exception {
		try {
			String methodName = Utility.getMethodName();
			LoggingUtilities.banner(log, SubscribeTests.class, methodName);

			serverURI = TestProperties.getServerURI();
			topicPrefix = "BasicTest-" + UUID.randomUUID().toString() + "-";
		} catch (Exception exception) {
			log.log(Level.SEVERE, "caught exception:", exception);
			throw exception;
		}
	}
	
	public static URI getServerURI() {
		return serverURI;
	}
	
	public static String getTopicPrefix() {
		return topicPrefix;
	}

}

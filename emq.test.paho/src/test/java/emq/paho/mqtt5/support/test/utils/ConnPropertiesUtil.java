package emq.paho.mqtt5.support.test.utils;

import java.lang.reflect.Field;

import org.eclipse.paho.mqttv5.client.MqttAsyncClient;
import org.eclipse.paho.mqttv5.client.internal.MqttConnectionState;

public class ConnPropertiesUtil {
	
	public static long getKeepAlive(MqttAsyncClient asyncClient) throws Exception {
		MqttConnectionState mqttConn = getMqttConnFromClient(asyncClient);
		return mqttConn.getKeepAlive();
	}
	
	public static MqttConnectionState getMqttConnFromClient(MqttAsyncClient asyncClient) throws Exception {
		Field field = asyncClient.getClass().getDeclaredField("mqttConnection");
		field.setAccessible(true);
		MqttConnectionState mqttConn = (MqttConnectionState) field.get(asyncClient);
		return mqttConn;
	}

}

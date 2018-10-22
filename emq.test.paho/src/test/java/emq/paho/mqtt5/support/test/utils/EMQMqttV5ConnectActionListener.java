package emq.paho.mqtt5.support.test.utils;

import org.eclipse.paho.mqttv5.client.IMqttToken;
import org.eclipse.paho.mqttv5.client.MqttActionListener;
import org.eclipse.paho.mqttv5.common.packet.MqttConnAck;
import org.eclipse.paho.mqttv5.common.packet.MqttWireMessage;

public class EMQMqttV5ConnectActionListener implements MqttActionListener {
	
	private MqttConnAck connAck;

	public void onSuccess(IMqttToken asyncActionToken) {
		MqttWireMessage mqttMsg = asyncActionToken.getResponse();
		if (mqttMsg instanceof MqttConnAck) {
			connAck = (MqttConnAck) mqttMsg;
		}
	}

	public void onFailure(IMqttToken asyncActionToken, Throwable exception) {
		MqttWireMessage mqttMsg = asyncActionToken.getResponse();
		if (mqttMsg instanceof MqttConnAck) {
			connAck = (MqttConnAck) mqttMsg;
		}
	}
	
	public MqttConnAck getConnAck() {
		return connAck;
	}

}

package com.ztesoft.zsmart.mq.zmq.namesrv;

import com.ztesoft.zsmart.zmq.common.MixAll;
import com.ztesoft.zsmart.zmq.common.protocol.RequestCode;
import com.ztesoft.zsmart.zmq.common.protocol.header.namesrv.RegisterBrokerRequestHeader;
import com.ztesoft.zsmart.zmq.remoting.RemotingClient;
import com.ztesoft.zsmart.zmq.remoting.protocol.RemotingCommand;
import com.ztesoft.zsmart.zmq.remoting.protocol.RemotingSerializable;

public class TestRegisterBroker extends TestNameServBase {

	public static void main(String[] args) {
		  RemotingClient client = createRemotingClient();

	        RegisterBrokerRequestHeader request = new RegisterBrokerRequestHeader();
	        request.setClusterName("default_cluster");
	        request.setBrokerName("broker_1");
	        request.setBrokerId(0L);
	        request.setBrokerAddr(MixAll.getLocalInetAddress().get(0));
	        request.setHaServerAddr(MixAll.getLocalInetAddress().get(0));
	        RemotingCommand cmd = RemotingCommand.createRequestCommand(RequestCode.REGISTER_BROKER, request);
	        try {
	            RemotingCommand response = client.invokeSync("localhost:1234", cmd, 1000L);
	           System.out.println(RemotingSerializable.toJson(response, false));
	        }
	        catch (Exception e) {
	            // TODO Auto-generated catch block
	            e.printStackTrace();
	        }
	}

}

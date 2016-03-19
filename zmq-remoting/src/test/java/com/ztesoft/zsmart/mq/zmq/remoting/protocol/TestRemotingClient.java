package com.ztesoft.zsmart.mq.zmq.remoting.protocol;

import com.ztesoft.zsmart.zmq.remoting.RemotingClient;
import com.ztesoft.zsmart.zmq.remoting.netty.NettyClientConfig;
import com.ztesoft.zsmart.zmq.remoting.netty.NettyRemotingClient;
import com.ztesoft.zsmart.zmq.remoting.protocol.RemotingCommand;
import com.ztesoft.zsmart.zmq.remoting.protocol.RemotingSerializable;

public class TestRemotingClient {
	public static void main(String[] args){
		RemotingClient client = createRemotingClient();
		RemotingCommand request = RemotingCommand.createRequestCommand(0, null);
		
		try {
			RemotingCommand response = client.invokeSync("localhost:8888", request, 1000 * 3000);
			
			System.out.println(RemotingSerializable.toJson(response, false));
        }catch(Exception e){
        	e.printStackTrace();
        }
	}
	
	private static RemotingClient createRemotingClient() {
        NettyClientConfig config = new NettyClientConfig();
        RemotingClient client = new NettyRemotingClient(config);
        client.start();
        return client;
    }
}

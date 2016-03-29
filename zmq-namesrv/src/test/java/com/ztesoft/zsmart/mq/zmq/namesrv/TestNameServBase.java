package com.ztesoft.zsmart.mq.zmq.namesrv;

import com.ztesoft.zsmart.zmq.remoting.RemotingClient;
import com.ztesoft.zsmart.zmq.remoting.netty.NettyClientConfig;
import com.ztesoft.zsmart.zmq.remoting.netty.NettyRemotingClient;

public class TestNameServBase {
    public static RemotingClient  createRemotingClient(){
        NettyClientConfig config = new NettyClientConfig();
        NettyRemotingClient client = new NettyRemotingClient(config);
        client.start();
        return client;
    }
}

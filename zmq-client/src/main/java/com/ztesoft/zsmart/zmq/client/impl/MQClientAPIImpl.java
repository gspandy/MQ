package com.ztesoft.zsmart.zmq.client.impl;

import java.util.List;

import org.slf4j.Logger;

import com.ztesoft.zsmart.zmq.client.log.ClientLogger;
import com.ztesoft.zsmart.zmq.common.MQVersion;
import com.ztesoft.zsmart.zmq.common.MixAll;
import com.ztesoft.zsmart.zmq.common.namesrv.TopAddressing;
import com.ztesoft.zsmart.zmq.remoting.RPCHook;
import com.ztesoft.zsmart.zmq.remoting.RemotingClient;
import com.ztesoft.zsmart.zmq.remoting.netty.NettyClientConfig;
import com.ztesoft.zsmart.zmq.remoting.netty.NettyRemotingClient;
import com.ztesoft.zsmart.zmq.remoting.protocol.RemotingCommand;

public class MQClientAPIImpl {
    static {
        System.setProperty(RemotingCommand.RemotingVersionKey, Integer.toString(MQVersion.CurrentVersion));
    }

    private final static Logger log = ClientLogger.getLog();

    private final RemotingClient remotingClient;

    private final TopAddressing topAddressing = new TopAddressing(MixAll.WS_ADDR);

    private final ClientRemotingProcessor clientRemotingProcessor;

    private String nameSrvAddr = null;

    private String projectGroupPrefix;

    public MQClientAPIImpl(final NettyClientConfig nettyClientConfig,
        final ClientRemotingProcessor clientRemotingProcessor, RPCHook rpcHook) {
        this.remotingClient = new NettyRemotingClient(nettyClientConfig);
        this.clientRemotingProcessor = clientRemotingProcessor;
        this.remotingClient.registerRPCHook(rpcHook);

    }

    public MQClientAPIImpl(final NettyClientConfig nettyClientConfig,
        final ClientRemotingProcessor clientRemotingProcessor) {
        this(nettyClientConfig, clientRemotingProcessor, null);
    }
    
    public List<String> getNameServerAddressList() {
        return this.remotingClient.getNameServerAddressList();
    }
}

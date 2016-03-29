package com.ztesoft.zsmart.zmq.client.impl;

import org.slf4j.Logger;

import io.netty.channel.ChannelHandlerContext;

import com.ztesoft.zsmart.zmq.client.impl.factory.MQClientInstance;
import com.ztesoft.zsmart.zmq.client.log.ClientLogger;
import com.ztesoft.zsmart.zmq.remoting.netty.NettyRequestProcessor;
import com.ztesoft.zsmart.zmq.remoting.protocol.RemotingCommand;

/**
 * 客户端通信处理程序 <br>
 * 
 * @author wang.jun<br>
 * @version 1.0<br>
 * @taskId <br>
 * @CreateDate 2016年3月29日 <br>
 * @since V7.3<br>
 * @see com.ztesoft.zsmart.zmq.client.impl <br>
 */
public class ClientRemotingProcessor implements NettyRequestProcessor {
    private final Logger log = ClientLogger.getLog();

    private final MQClientInstance mqClientFactory;

    public ClientRemotingProcessor(final MQClientInstance mqClientFactory) {
        this.mqClientFactory = mqClientFactory;
    }

    @Override
    public RemotingCommand processRequest(ChannelHandlerContext ctx, RemotingCommand request) throws Exception {
        // TODO Auto-generated method stub
        return null;
    }

}

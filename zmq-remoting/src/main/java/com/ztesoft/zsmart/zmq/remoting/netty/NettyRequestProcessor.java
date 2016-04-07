package com.ztesoft.zsmart.zmq.remoting.netty;

import com.ztesoft.zsmart.zmq.remoting.exception.RemotingCommandException;
import com.ztesoft.zsmart.zmq.remoting.protocol.RemotingCommand;

import io.netty.channel.ChannelHandlerContext;

public interface NettyRequestProcessor {
	RemotingCommand processRequest(ChannelHandlerContext ctx, RemotingCommand request) throws RemotingCommandException;
}

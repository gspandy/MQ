package com.ztesoft.zsmart.mq.zmq.remoting.protocol;

import java.util.concurrent.Executors;

import com.ztesoft.zsmart.zmq.remoting.netty.NettyRemotingServer;
import com.ztesoft.zsmart.zmq.remoting.netty.NettyRequestProcessor;
import com.ztesoft.zsmart.zmq.remoting.netty.NettyServerConfig;
import com.ztesoft.zsmart.zmq.remoting.protocol.RemotingCommand;

import io.netty.channel.ChannelHandlerContext;

public class TestRemotingServer {

	public static void main(String[] args) {
		NettyServerConfig nettyServerConfig = new NettyServerConfig();
		 NettyRemotingServer server = new NettyRemotingServer(nettyServerConfig);
		 server.registerProcessor(0, new NettyRequestProcessor() {
	            private int i = 0;


	            @Override
	            public RemotingCommand processRequest(ChannelHandlerContext ctx, RemotingCommand request) {
	                System.out.println("processRequest=" + request + " " + (i++));
	                request.setRemark("hello, I am respponse " + ctx.channel().remoteAddress());
	                return request;
	            }
	        }, Executors.newCachedThreadPool());
		 server.start();

	}

}

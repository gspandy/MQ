package com.ztesoft.zsmart.mq.zmq.remoting.protocol;

import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;

import com.ztesoft.zsmart.zmq.remoting.ChannelEventListener;
import com.ztesoft.zsmart.zmq.remoting.RemotingClient;
import com.ztesoft.zsmart.zmq.remoting.RemotingServer;
import com.ztesoft.zsmart.zmq.remoting.netty.NettyClientConfig;
import com.ztesoft.zsmart.zmq.remoting.netty.NettyRemotingClient;
import com.ztesoft.zsmart.zmq.remoting.netty.NettyRemotingServer;
import com.ztesoft.zsmart.zmq.remoting.netty.NettyRequestProcessor;
import com.ztesoft.zsmart.zmq.remoting.netty.NettyServerConfig;
import com.ztesoft.zsmart.zmq.remoting.protocol.RemotingCommand;
import com.ztesoft.zsmart.zmq.remoting.protocol.RemotingSerializable;

public class TestRemotingIdle {
    public static void main(String[] args){
        RemotingClient client = createRemotingClient();
        //RemotingServer server = createRemotingServer();
        
        RemotingCommand request = RemotingCommand.createRequestCommand(0, null);
        
        try {
            
            RemotingCommand response = client.invokeSync("localhost:8888", request, 1000 * 3000);
            TimeUnit.SECONDS.sleep(20);
            System.out.println(RemotingSerializable.toJson(response, false));
        }catch(Exception e){
            e.printStackTrace();
        }
    }
    
    
    private static RemotingServer createRemotingServer(){
        NettyServerConfig config = new NettyServerConfig();
        config.setServerChannelMaxIdleTimeSeconds(30);
        RemotingServer remotingServer = new NettyRemotingServer(config);
        remotingServer.registerProcessor(0, new NettyRequestProcessor() {
            private int i = 0;


            @Override
            public RemotingCommand processRequest(ChannelHandlerContext ctx, RemotingCommand request) {
                System.out.println("processRequest=" + request + " " + (i++));
                request.setRemark("hello, I am respponse " + ctx.channel().remoteAddress());
                return request;
            }
        }, Executors.newCachedThreadPool());
        remotingServer.start();
        return remotingServer;
    }
    
    private static RemotingClient createRemotingClient() {
        NettyClientConfig config = new NettyClientConfig();
        config.setClientChannelMaxIdleTimeSeconds(15);
        RemotingClient client = new NettyRemotingClient(config,new ChannelEventListener() {
            
            @Override
            public void onChannelIdle(String remoteAddr, Channel channel) {
                System.out.println("channel idle ==============="+remoteAddr);
            }
            
            @Override
            public void onChannelException(String remoteAddr, Channel channel) {
                
            }
            
            @Override
            public void onChannelConnect(String remoteAddr, Channel channel) {
                
            }
            
            @Override
            public void onChannelClose(String remoteAddr, Channel channel) {
                
            }
        });
        client.start();
        return client;
    }
}

package com.ztesoft.zsmart.mq.zmq.remoting.protocol;

import com.ztesoft.zsmart.zmq.remoting.RemotingClient;
import com.ztesoft.zsmart.zmq.remoting.exception.RemotingConnectException;
import com.ztesoft.zsmart.zmq.remoting.exception.RemotingSendRequestException;
import com.ztesoft.zsmart.zmq.remoting.exception.RemotingTimeoutException;
import com.ztesoft.zsmart.zmq.remoting.netty.NettyClientConfig;
import com.ztesoft.zsmart.zmq.remoting.netty.NettyRemotingClient;
import com.ztesoft.zsmart.zmq.remoting.protocol.RemotingCommand;

public class TestNettyConnection {

    public static void main(String[] args) {
         RemotingClient client = createRemotingClient();
         
         for(int i=0;i<10;i++){
             RemotingCommand cmd = RemotingCommand.createRequestCommand(0, null);
             try {
                client.invokeSync("localhost:8888", cmd, 3000);
            }
            catch (RemotingConnectException e) {
                e.printStackTrace();
            }
            catch (RemotingSendRequestException e) {
                e.printStackTrace();
            }
            catch (RemotingTimeoutException e) {
                e.printStackTrace();
            }
            catch (InterruptedException e) {
                e.printStackTrace();
            }
         }
         System.out.println("==================================================");
         client.shutdown();
    }
    
    private static RemotingClient createRemotingClient() {
        NettyClientConfig config = new NettyClientConfig();
        config.setClientChannelMaxIdleTimeSeconds(15);
        RemotingClient client = new NettyRemotingClient(config);
        client.start();
        return client;
    }

}

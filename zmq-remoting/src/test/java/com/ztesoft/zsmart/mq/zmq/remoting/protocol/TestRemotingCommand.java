package com.ztesoft.zsmart.mq.zmq.remoting.protocol;

import java.nio.ByteBuffer;

import com.ztesoft.zsmart.zmq.remoting.protocol.RemotingCommand;

public class TestRemotingCommand {

    public static void main(String[] args) {
        RemotingCommand cmd =  RemotingCommand.createRequestCommand(1, null);
        
        ByteBuffer bf = cmd.encode();
        
        int length = bf.limit();
        
        byte[] data = new byte[length];
        bf.get(data);
        
       System.out.println( RemotingCommand.decode(data).getCode());

    }

}

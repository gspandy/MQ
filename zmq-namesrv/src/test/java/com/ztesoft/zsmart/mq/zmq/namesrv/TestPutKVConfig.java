package com.ztesoft.zsmart.mq.zmq.namesrv;

import com.ztesoft.zsmart.zmq.common.protocol.RequestCode;
import com.ztesoft.zsmart.zmq.common.protocol.header.namesrv.PutKVConfigRequestHeader;
import com.ztesoft.zsmart.zmq.remoting.RemotingClient;
import com.ztesoft.zsmart.zmq.remoting.protocol.RemotingCommand;
import com.ztesoft.zsmart.zmq.remoting.protocol.RemotingSerializable;

public class TestPutKVConfig extends TestNameServBase{

    public static void main(String[] args) {
        RemotingClient client = createRemotingClient();
        PutKVConfigRequestHeader header = new PutKVConfigRequestHeader();
        header.setNamespace("com.ztesoft.com");
        header.setKey("wangjun");
        header.setValue("0027008609");
        
        RemotingCommand cmd = RemotingCommand.createRequestCommand(RequestCode.PUT_KV_CONFIG, header);
        try {
            RemotingCommand response = client.invokeSync("localhost:1234", cmd, 10000);
            System.out.println(RemotingSerializable.toJson(response, false));
        }
        catch (Exception e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        } 
        

    }

}

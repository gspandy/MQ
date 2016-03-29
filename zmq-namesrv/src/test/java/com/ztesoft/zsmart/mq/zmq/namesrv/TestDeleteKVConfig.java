package com.ztesoft.zsmart.mq.zmq.namesrv;

import com.ztesoft.zsmart.zmq.common.protocol.RequestCode;
import com.ztesoft.zsmart.zmq.common.protocol.header.namesrv.DeleteKVConfigRequestHeader;
import com.ztesoft.zsmart.zmq.remoting.RemotingClient;
import com.ztesoft.zsmart.zmq.remoting.protocol.RemotingCommand;
import com.ztesoft.zsmart.zmq.remoting.protocol.RemotingSerializable;

public class TestDeleteKVConfig extends TestNameServBase {

    public static void main(String[] args) {
        RemotingClient client = createRemotingClient();

        DeleteKVConfigRequestHeader request = new DeleteKVConfigRequestHeader();
        request.setKey("wangjun");
        request.setNamespace("com.ztesoft.com");

        RemotingCommand cmd = RemotingCommand.createRequestCommand(RequestCode.DELETE_KV_CONFIG, request);

        try {
            RemotingCommand response = client.invokeSync("localhost:1234", cmd, 1000L);
           System.out.println(RemotingSerializable.toJson(response, false));
        }
        catch (Exception e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
         
    }
}

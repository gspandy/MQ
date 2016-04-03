package com.ztesoft.zsmart.zmq.common.protocol.header;

import com.ztesoft.zsmart.zmq.remoting.CommandCustomHeader;
import com.ztesoft.zsmart.zmq.remoting.annotation.CFNotNull;
import com.ztesoft.zsmart.zmq.remoting.annotation.CFNullable;
import com.ztesoft.zsmart.zmq.remoting.exception.RemotingCommandException;


/**
 * 查看客户端消费组的消费情况。
 * 
 * @author J.Wang
 *
 */
public class GetConsumerStatusRequestHeader implements CommandCustomHeader {
    @CFNotNull
    private String topic;
    @CFNotNull
    private String group;
    @CFNullable
    private String clientAddr;


    @Override
    public void checkFields() throws RemotingCommandException {

    }


    public String getTopic() {
        return topic;
    }


    public void setTopic(String topic) {
        this.topic = topic;
    }


    public String getGroup() {
        return group;
    }


    public void setGroup(String group) {
        this.group = group;
    }


    public String getClientAddr() {
        return clientAddr;
    }


    public void setClientAddr(String clientAddr) {
        this.clientAddr = clientAddr;
    }

}

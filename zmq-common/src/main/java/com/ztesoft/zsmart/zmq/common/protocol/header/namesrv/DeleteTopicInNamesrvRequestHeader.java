package com.ztesoft.zsmart.zmq.common.protocol.header.namesrv;

import com.ztesoft.zsmart.zmq.remoting.CommandCustomHeader;
import com.ztesoft.zsmart.zmq.remoting.annotation.CFNotNull;
import com.ztesoft.zsmart.zmq.remoting.exception.RemotingCommandException;

public class DeleteTopicInNamesrvRequestHeader implements CommandCustomHeader {

    @CFNotNull
    private String topic;

    @Override
    public void checkFields() throws RemotingCommandException {

    }

    public String getTopic() {
        return topic;
    }

    public void setTopic(String topic) {
        this.topic = topic;
    }

}

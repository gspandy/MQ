package com.ztesoft.zsmart.zmq.common.protocol.header.namesrv;

import com.ztesoft.zsmart.zmq.remoting.CommandCustomHeader;
import com.ztesoft.zsmart.zmq.remoting.annotation.CFNotNull;
import com.ztesoft.zsmart.zmq.remoting.exception.RemotingCommandException;

public class WipeWritePermOfBrokerResponseHeader implements CommandCustomHeader {

    @CFNotNull
    private Integer wipeTopicCount;

    @Override
    public void checkFields() throws RemotingCommandException {

    }

    public Integer getWipeTopicCount() {
        return wipeTopicCount;
    }

    public void setWipeTopicCount(Integer wipeTopicCount) {
        this.wipeTopicCount = wipeTopicCount;
    }

}

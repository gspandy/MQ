package com.ztesoft.zsmart.zmq.common.protocol.header.namesrv;

import com.ztesoft.zsmart.zmq.remoting.CommandCustomHeader;
import com.ztesoft.zsmart.zmq.remoting.annotation.CFNotNull;
import com.ztesoft.zsmart.zmq.remoting.exception.RemotingCommandException;

public class GetTopicsByClusterRequestHeader implements CommandCustomHeader {

    @CFNotNull
    private String cluster;

    @Override
    public void checkFields() throws RemotingCommandException {

    }

    public String getCluster() {
        return cluster;
    }

    public void setCluster(String cluster) {
        this.cluster = cluster;
    }

}

package com.ztesoft.zsmart.zmq.common.protocol.filtersrv;

import com.ztesoft.zsmart.zmq.remoting.CommandCustomHeader;
import com.ztesoft.zsmart.zmq.remoting.annotation.CFNotNull;
import com.ztesoft.zsmart.zmq.remoting.exception.RemotingCommandException;


public class RegisterFilterServerResponseHeader implements CommandCustomHeader {
    @CFNotNull
    private String brokerName;
    @CFNotNull
    private long brokerId;


    @Override
    public void checkFields() throws RemotingCommandException {
    }


    public long getBrokerId() {
        return brokerId;
    }


    public void setBrokerId(long brokerId) {
        this.brokerId = brokerId;
    }


    public String getBrokerName() {
        return brokerName;
    }


    public void setBrokerName(String brokerName) {
        this.brokerName = brokerName;
    }
}

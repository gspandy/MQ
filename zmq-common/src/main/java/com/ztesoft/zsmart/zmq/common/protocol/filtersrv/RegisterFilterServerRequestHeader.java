package com.ztesoft.zsmart.zmq.common.protocol.filtersrv;

import com.ztesoft.zsmart.zmq.remoting.CommandCustomHeader;
import com.ztesoft.zsmart.zmq.remoting.annotation.CFNotNull;
import com.ztesoft.zsmart.zmq.remoting.exception.RemotingCommandException;


public class RegisterFilterServerRequestHeader implements CommandCustomHeader {
    @CFNotNull
    private String filterServerAddr;


    @Override
    public void checkFields() throws RemotingCommandException {
    }


    public String getFilterServerAddr() {
        return filterServerAddr;
    }


    public void setFilterServerAddr(String filterServerAddr) {
        this.filterServerAddr = filterServerAddr;
    }
}

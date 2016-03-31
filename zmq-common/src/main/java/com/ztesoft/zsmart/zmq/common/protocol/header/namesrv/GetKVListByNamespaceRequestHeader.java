package com.ztesoft.zsmart.zmq.common.protocol.header.namesrv;

import com.ztesoft.zsmart.zmq.remoting.CommandCustomHeader;
import com.ztesoft.zsmart.zmq.remoting.annotation.CFNotNull;
import com.ztesoft.zsmart.zmq.remoting.exception.RemotingCommandException;

public class GetKVListByNamespaceRequestHeader implements CommandCustomHeader {

    @CFNotNull
    private String namespace;

    @Override
    public void checkFields() throws RemotingCommandException {

    }

    public String getNamespace() {
        return namespace;
    }

    public void setNamespace(String namespace) {
        this.namespace = namespace;
    }

}

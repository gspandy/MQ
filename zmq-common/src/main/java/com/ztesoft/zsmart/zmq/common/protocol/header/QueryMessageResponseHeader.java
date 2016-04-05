package com.ztesoft.zsmart.zmq.common.protocol.header;

import com.ztesoft.zsmart.zmq.remoting.CommandCustomHeader;
import com.ztesoft.zsmart.zmq.remoting.annotation.CFNotNull;
import com.ztesoft.zsmart.zmq.remoting.exception.RemotingCommandException;

public class QueryMessageResponseHeader implements CommandCustomHeader {

    @CFNotNull
    private Long indexLastUpdateTimestamp;

    @CFNotNull
    private Long indexLastUpdatePhyoffset;

    @Override
    public void checkFields() throws RemotingCommandException {

    }

    public Long getIndexLastUpdateTimestamp() {
        return indexLastUpdateTimestamp;
    }

    public void setIndexLastUpdateTimestamp(Long indexLastUpdateTimestamp) {
        this.indexLastUpdateTimestamp = indexLastUpdateTimestamp;
    }

    public Long getIndexLastUpdatePhyoffset() {
        return indexLastUpdatePhyoffset;
    }

    public void setIndexLastUpdatePhyoffset(Long indexLastUpdatePhyoffset) {
        this.indexLastUpdatePhyoffset = indexLastUpdatePhyoffset;
    }

}

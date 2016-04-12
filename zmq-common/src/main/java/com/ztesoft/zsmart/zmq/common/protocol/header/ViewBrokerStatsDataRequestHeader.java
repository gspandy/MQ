package com.ztesoft.zsmart.zmq.common.protocol.header;

import com.ztesoft.zsmart.zmq.remoting.CommandCustomHeader;
import com.ztesoft.zsmart.zmq.remoting.annotation.CFNotNull;
import com.ztesoft.zsmart.zmq.remoting.exception.RemotingCommandException;


public class ViewBrokerStatsDataRequestHeader implements CommandCustomHeader {
    @CFNotNull
    private String statsName;
    @CFNotNull
    private String statsKey;


    @Override
    public void checkFields() throws RemotingCommandException {

    }


    public String getStatsName() {
        return statsName;
    }


    public void setStatsName(String statsName) {
        this.statsName = statsName;
    }


    public String getStatsKey() {
        return statsKey;
    }


    public void setStatsKey(String statsKey) {
        this.statsKey = statsKey;
    }
}

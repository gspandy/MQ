package com.ztesoft.zsmart.zmq.common.protocol.body;

import java.util.concurrent.ConcurrentHashMap;

import com.ztesoft.zsmart.zmq.common.DataVersion;
import com.ztesoft.zsmart.zmq.common.subscription.SubscriptionGroupConfig;
import com.ztesoft.zsmart.zmq.remoting.protocol.RemotingSerializable;

public class SubscriptionGroupWrapper extends RemotingSerializable {
    private ConcurrentHashMap<String, SubscriptionGroupConfig> subscriptionGroupTable = new ConcurrentHashMap<String, SubscriptionGroupConfig>();

    private DataVersion dataVersion = new DataVersion();

    public ConcurrentHashMap<String, SubscriptionGroupConfig> getSubscriptionGroupTable() {
        return subscriptionGroupTable;
    }

    public void setSubscriptionGroupTable(ConcurrentHashMap<String, SubscriptionGroupConfig> subscriptionGroupTable) {
        this.subscriptionGroupTable = subscriptionGroupTable;
    }

    public DataVersion getDataVersion() {
        return dataVersion;
    }

    public void setDataVersion(DataVersion dataVersion) {
        this.dataVersion = dataVersion;
    }
}

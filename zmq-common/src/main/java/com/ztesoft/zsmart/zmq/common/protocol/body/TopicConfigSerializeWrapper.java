package com.ztesoft.zsmart.zmq.common.protocol.body;

import java.util.concurrent.ConcurrentHashMap;

import com.ztesoft.zsmart.zmq.common.DataVersion;
import com.ztesoft.zsmart.zmq.common.TopicConfig;
import com.ztesoft.zsmart.zmq.remoting.protocol.RemotingSerializable;

public class TopicConfigSerializeWrapper extends RemotingSerializable {
    private ConcurrentHashMap<String, TopicConfig> topicConfigTable = new ConcurrentHashMap<String, TopicConfig>();

    private DataVersion dataVersion = new DataVersion();

    public ConcurrentHashMap<String, TopicConfig> getTopicConfigTable() {
        return topicConfigTable;
    }

    public void setTopicConfigTable(ConcurrentHashMap<String, TopicConfig> topicConfigTable) {
        this.topicConfigTable = topicConfigTable;
    }

    public DataVersion getDataVersion() {
        return dataVersion;
    }

    public void setDataVersion(DataVersion dataVersion) {
        this.dataVersion = dataVersion;
    }

}

package com.ztesoft.zsmart.zmq.common.protocol.body;

import java.util.HashSet;
import java.util.Set;

import com.ztesoft.zsmart.zmq.common.message.MessageQueue;
import com.ztesoft.zsmart.zmq.remoting.protocol.RemotingSerializable;


/**
 * @author shijia.wxr<vintage.wang@gmail.com>
 * @since 2013-6-26
 */
public class LockBatchRequestBody extends RemotingSerializable {
    private String consumerGroup;
    private String clientId;
    private Set<MessageQueue> mqSet = new HashSet<MessageQueue>();


    public String getConsumerGroup() {
        return consumerGroup;
    }


    public void setConsumerGroup(String consumerGroup) {
        this.consumerGroup = consumerGroup;
    }


    public String getClientId() {
        return clientId;
    }


    public void setClientId(String clientId) {
        this.clientId = clientId;
    }


    public Set<MessageQueue> getMqSet() {
        return mqSet;
    }


    public void setMqSet(Set<MessageQueue> mqSet) {
        this.mqSet = mqSet;
    }
}

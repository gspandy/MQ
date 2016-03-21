package com.ztesoft.zsmart.zmq.common.protocol.topic;

import com.ztesoft.zsmart.zmq.common.message.MessageQueue;
import com.ztesoft.zsmart.zmq.remoting.protocol.RemotingSerializable;

public class OffsetMovedEvent extends RemotingSerializable {
    private String consumerGroup;

    private MessageQueue messageQueue;

    /**
     * 客户端请求Offset
     */
    private long offsetRequest;

    private long offsetNew;

    public String getConsumerGroup() {
        return consumerGroup;
    }

    public void setConsumerGroup(String consumerGroup) {
        this.consumerGroup = consumerGroup;
    }

    public MessageQueue getMessageQueue() {
        return messageQueue;
    }

    public void setMessageQueue(MessageQueue messageQueue) {
        this.messageQueue = messageQueue;
    }

    public long getOffsetRequest() {
        return offsetRequest;
    }

    public void setOffsetRequest(long offsetRequest) {
        this.offsetRequest = offsetRequest;
    }

    public long getOffsetNew() {
        return offsetNew;
    }

    public void setOffsetNew(long offsetNew) {
        this.offsetNew = offsetNew;
    }

    @Override
    public String toString() {
        return "OffsetMovedEvent [consumerGroup=" + consumerGroup + ", messageQueue=" + messageQueue
            + ", offsetRequest=" + offsetRequest + ", offsetNew=" + offsetNew + "]";
    }
}

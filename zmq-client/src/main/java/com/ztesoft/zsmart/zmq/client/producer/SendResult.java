package com.ztesoft.zsmart.zmq.client.producer;

import com.ztesoft.zsmart.zmq.client.VirtualEnvUtil;
import com.ztesoft.zsmart.zmq.common.UtilAll;
import com.ztesoft.zsmart.zmq.common.message.MessageQueue;


public class SendResult {
    private SendStatus sendStatus;
    private String msgId;
    private MessageQueue messageQueue;
    private long queueOffset;
    private String transactionId;


    public SendResult() {
    }


    public SendResult(SendStatus sendStatus, String msgId, MessageQueue messageQueue, long queueOffset,
            String projectGroupPrefix) {
        this.sendStatus = sendStatus;
        this.msgId = msgId;
        this.messageQueue = messageQueue;
        this.queueOffset = queueOffset;
        if (!UtilAll.isBlank(projectGroupPrefix)) {
            this.messageQueue
                .setTopic(VirtualEnvUtil.clearProjectGroup(this.messageQueue.getTopic(), projectGroupPrefix));
        }
    }


    public String getMsgId() {
        return msgId;
    }


    public void setMsgId(String msgId) {
        this.msgId = msgId;
    }


    public SendStatus getSendStatus() {
        return sendStatus;
    }


    public void setSendStatus(SendStatus sendStatus) {
        this.sendStatus = sendStatus;
    }


    public MessageQueue getMessageQueue() {
        return messageQueue;
    }


    public void setMessageQueue(MessageQueue messageQueue) {
        this.messageQueue = messageQueue;
    }


    public long getQueueOffset() {
        return queueOffset;
    }


    public void setQueueOffset(long queueOffset) {
        this.queueOffset = queueOffset;
    }


    public String getTransactionId() {
        return transactionId;
    }


    public void setTransactionId(String transactionId) {
        this.transactionId = transactionId;
    }


    @Override
    public String toString() {
        return "SendResult [sendStatus=" + sendStatus + ", msgId=" + msgId + ", messageQueue=" + messageQueue
                + ", queueOffset=" + queueOffset + "]";
    }
}

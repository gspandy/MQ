package com.ztesoft.zsmart.zmq.client.impl.consumer;

import java.util.List;

import com.ztesoft.zsmart.zmq.common.message.MessageExt;


public class PullResultExt extends PullResult {
    private final long suggestWhichBrokerId;
    private byte[] messageBinary;


    public PullResultExt(PullStatus pullStatus, long nextBeginOffset, long minOffset, long maxOffset,
            List<MessageExt> msgFoundList, final long suggestWhichBrokerId, final byte[] messageBinary) {
        super(pullStatus, nextBeginOffset, minOffset, maxOffset, msgFoundList);
        this.suggestWhichBrokerId = suggestWhichBrokerId;
        this.messageBinary = messageBinary;
    }


    public byte[] getMessageBinary() {
        return messageBinary;
    }


    public void setMessageBinary(byte[] messageBinary) {
        this.messageBinary = messageBinary;
    }


    public long getSuggestWhichBrokerId() {
        return suggestWhichBrokerId;
    }
}

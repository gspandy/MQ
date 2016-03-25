package com.ztesoft.zsmart.zmq.client.producer.selector;

import java.util.List;

import com.ztesoft.zsmart.zmq.client.producer.MessageQueueSelector;
import com.ztesoft.zsmart.zmq.common.message.Message;
import com.ztesoft.zsmart.zmq.common.message.MessageQueue;

public class SelectMessageQueueByHash implements MessageQueueSelector {

    @Override
    public MessageQueue select(List<MessageQueue> mqs, Message msg, Object arg) {
        int value = Math.abs(arg.hashCode());

        value = value % mqs.size();
        return mqs.get(value);
    }

}

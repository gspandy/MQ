package com.ztesoft.zsmart.zmq.client.impl.producer;

import java.util.Set;

import com.ztesoft.zsmart.zmq.client.producer.TransactionCheckListener;
import com.ztesoft.zsmart.zmq.common.message.MessageExt;
import com.ztesoft.zsmart.zmq.common.protocol.header.namesrv.CheckTransactionStateRequestHeader;

public class DefaultMQProducerImpl implements MQProducerInner {

    @Override
    public Set<String> getPublishTopicList() {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public boolean isPublishTopicNeedUpdate(String topic) {
        // TODO Auto-generated method stub
        return false;
    }

    @Override
    public TransactionCheckListener checkListener() {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public void checkTransactionState(String addr, MessageExt msg, CheckTransactionStateRequestHeader checkRequestHeader) {
        // TODO Auto-generated method stub

    }

    @Override
    public void updateTopicPublishInfo(String topic, TopicPublishInfo info) {
        // TODO Auto-generated method stub

    }

    @Override
    public boolean isUnitMode() {
        // TODO Auto-generated method stub
        return false;
    }

}

package com.ztesoft.zsmart.zmq.client.impl.producer;

import java.util.Set;

import com.ztesoft.zsmart.zmq.client.producer.TransactionCheckListener;
import com.ztesoft.zsmart.zmq.common.message.MessageExt;
import com.ztesoft.zsmart.zmq.common.protocol.header.CheckTransactionStateRequestHeader;

public interface MQProducerInner {
    Set<String> getPublishTopicList();

    boolean isPublishTopicNeedUpdate(final String topic);

    TransactionCheckListener checkListener();

    void checkTransactionState(//
        final String addr, //
        final MessageExt msg, //
        final CheckTransactionStateRequestHeader checkRequestHeader);

    void updateTopicPublishInfo(final String topic, final TopicPublishInfo info);

    boolean isUnitMode();
}

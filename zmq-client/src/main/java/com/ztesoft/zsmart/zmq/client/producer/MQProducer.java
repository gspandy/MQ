package com.ztesoft.zsmart.zmq.client.producer;

import java.util.List;

import com.ztesoft.zsmart.zmq.client.MQAdmin;
import com.ztesoft.zsmart.zmq.client.exception.MQBrokerException;
import com.ztesoft.zsmart.zmq.client.exception.MQClientException;
import com.ztesoft.zsmart.zmq.common.message.Message;
import com.ztesoft.zsmart.zmq.common.message.MessageQueue;
import com.ztesoft.zsmart.zmq.remoting.exception.RemotingException;

public interface MQProducer extends MQAdmin {
    void start() throws MQClientException;

    void shutdown();

    List<MessageQueue> fetchPublishMessageQueues(final String topic) throws MQClientException;

    SendResult send(final Message msg) throws MQClientException, RemotingException, MQBrokerException,
        InterruptedException;

    SendResult send(final Message msg, final long timeout) throws MQClientException, RemotingException,
        MQBrokerException, InterruptedException;

    void send(final Message msg, final SendCallback sendCallback) throws MQClientException, RemotingException,
        InterruptedException;

    void send(final Message msg, final SendCallback sendCallback, final long timeout) throws MQClientException,
        RemotingException, InterruptedException;

    void sendOneway(final Message msg) throws MQClientException, RemotingException, InterruptedException;

    SendResult send(final Message msg, final MessageQueue mq) throws MQClientException, RemotingException,
        MQBrokerException, InterruptedException;

    SendResult send(final Message msg, final MessageQueue mq, final long timeout) throws MQClientException,
        RemotingException, MQBrokerException, InterruptedException;

    void send(final Message msg, final MessageQueue mq, final SendCallback sendCallback) throws MQClientException,
        RemotingException, InterruptedException;

    void send(final Message msg, final MessageQueue mq, final SendCallback sendCallback, long timeout)
        throws MQClientException, RemotingException, InterruptedException;

    void sendOneway(final Message msg, final MessageQueue mq) throws MQClientException, RemotingException,
        InterruptedException;

    SendResult send(final Message msg, final MessageQueueSelector selector, final Object arg) throws MQClientException,
        RemotingException, MQBrokerException, InterruptedException;

    SendResult send(final Message msg, final MessageQueueSelector selector, final Object arg, final long timeout)
        throws MQClientException, RemotingException, MQBrokerException, InterruptedException;

    void send(final Message msg, final MessageQueueSelector selector, final Object arg, final SendCallback sendCallback)
        throws MQClientException, RemotingException, InterruptedException;

    void send(final Message msg, final MessageQueueSelector selector, final Object arg,
        final SendCallback sendCallback, final long timeout) throws MQClientException, RemotingException,
        InterruptedException;

    void sendOneway(final Message msg, final MessageQueueSelector selector, final Object arg) throws MQClientException,
        RemotingException, InterruptedException;

    TransactionSendResult sendMessageInTransaction(final Message msg, final LocalTransactionExecuter tranExecuter,
        final Object arg) throws MQClientException;

}

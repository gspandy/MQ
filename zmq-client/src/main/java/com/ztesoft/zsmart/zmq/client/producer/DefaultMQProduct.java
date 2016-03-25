package com.ztesoft.zsmart.zmq.client.producer;

import java.util.List;

import com.ztesoft.zsmart.zmq.client.ClientConfig;
import com.ztesoft.zsmart.zmq.client.QueryResult;
import com.ztesoft.zsmart.zmq.client.exception.MQBrokerException;
import com.ztesoft.zsmart.zmq.client.exception.MQClientException;
import com.ztesoft.zsmart.zmq.common.message.Message;
import com.ztesoft.zsmart.zmq.common.message.MessageExt;
import com.ztesoft.zsmart.zmq.common.message.MessageQueue;
import com.ztesoft.zsmart.zmq.remoting.exception.RemotingException;

/**
 * 
 * 默认消息生产者 <br> 
 *  
 * @author wang.jun<br>
 * @version 1.0<br>
 * @taskId <br>
 * @CreateDate 2016年3月25日 <br>
 * @since V7.3<br>
 * @see com.ztesoft.zsmart.zmq.client.producer <br>
 */
public class DefaultMQProduct extends ClientConfig implements MQProducer {

    @Override
    public void createTopic(String key, String newTopic, int queueNum) throws MQClientException {
        // TODO Auto-generated method stub

    }

    @Override
    public void createTopic(String key, String newTopic, int queueNum, int topicSysFlag) throws MQClientException {
        // TODO Auto-generated method stub

    }

    @Override
    public long searchOffset(MessageQueue mq, long timestamp) throws MQClientException {
        // TODO Auto-generated method stub
        return 0;
    }

    @Override
    public long maxOffset(MessageQueue mq) throws MQClientException {
        // TODO Auto-generated method stub
        return 0;
    }

    @Override
    public long minOffset(MessageQueue mq) throws MQClientException {
        // TODO Auto-generated method stub
        return 0;
    }

    @Override
    public long earliestMsgStoreTime(MessageQueue mq) throws MQClientException {
        // TODO Auto-generated method stub
        return 0;
    }

    @Override
    public MessageExt viewMessage(String msgId) throws RemotingException, MQBrokerException, InterruptedException,
        MQClientException {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public QueryResult queryMessage(String topic, String key, int maxNum, long begin, long end)
        throws MQClientException, InterruptedException {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public void start() throws MQClientException {
        // TODO Auto-generated method stub

    }

    @Override
    public void shtudown() {
        // TODO Auto-generated method stub

    }

    @Override
    public List<MessageQueue> fetchPublishMessageQueues(String topic) throws MQClientException {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public SendResult send(Message msg) throws MQClientException, RemotingException, MQBrokerException,
        InterruptedException {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public SendResult send(Message msg, long timeout) throws MQClientException, RemotingException, MQBrokerException,
        InterruptedException {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public void send(Message msg, SendCallback sendCallback) throws MQClientException, RemotingException,
        InterruptedException {
        // TODO Auto-generated method stub

    }

    @Override
    public void send(Message msg, SendCallback sendCallback, long timeout) throws MQClientException, RemotingException,
        InterruptedException {
        // TODO Auto-generated method stub

    }

    @Override
    public void sendOneway(Message msg) throws MQClientException, RemotingException, InterruptedException {
        // TODO Auto-generated method stub

    }

    @Override
    public SendResult send(Message msg, MessageQueue mq) throws MQClientException, RemotingException,
        MQBrokerException, InterruptedException {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public SendResult send(Message msg, MessageQueue mq, long timeout) throws MQClientException, RemotingException,
        MQBrokerException, InterruptedException {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public void send(Message msg, MessageQueue mq, SendCallback sendCallback) throws MQClientException,
        RemotingException, InterruptedException {
        // TODO Auto-generated method stub

    }

    @Override
    public void send(Message msg, MessageQueue mq, SendCallback sendCallback, long timeout) throws MQClientException,
        RemotingException, InterruptedException {
        // TODO Auto-generated method stub

    }

    @Override
    public void sendOneway(Message msg, MessageQueue mq) throws MQClientException, RemotingException,
        InterruptedException {
        // TODO Auto-generated method stub

    }

    @Override
    public SendResult send(Message msg, MessageQueueSelector selector, Object arg) throws MQClientException,
        RemotingException, MQBrokerException, InterruptedException {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public SendResult send(Message msg, MessageQueueSelector selector, Object arg, long timeout)
        throws MQClientException, RemotingException, MQBrokerException, InterruptedException {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public void send(Message msg, MessageQueueSelector selector, Object arg, SendCallback sendCallback)
        throws MQClientException, RemotingException, InterruptedException {
        // TODO Auto-generated method stub

    }

    @Override
    public void send(Message msg, MessageQueueSelector selector, Object arg, SendCallback sendCallback, long timeout)
        throws MQClientException, RemotingException, InterruptedException {
        // TODO Auto-generated method stub

    }

    @Override
    public void sendOneway(Message msg, MessageQueueSelector selector, Object arg) throws MQClientException,
        RemotingException, InterruptedException {
        // TODO Auto-generated method stub

    }

    @Override
    public TransactionSendResult sendMessageInTransaction(Message msg, LocalTransactionExecuter tranExecuter, Object arg)
        throws MQClientException {
        // TODO Auto-generated method stub
        return null;
    }

}

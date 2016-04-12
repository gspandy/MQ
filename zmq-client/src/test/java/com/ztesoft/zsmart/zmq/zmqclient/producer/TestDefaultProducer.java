package com.ztesoft.zsmart.zmq.zmqclient.producer;

import java.util.concurrent.TimeUnit;

import com.ztesoft.zsmart.zmq.client.exception.MQBrokerException;
import com.ztesoft.zsmart.zmq.client.exception.MQClientException;
import com.ztesoft.zsmart.zmq.client.producer.DefaultMQProducer;
import com.ztesoft.zsmart.zmq.common.message.Message;
import com.ztesoft.zsmart.zmq.remoting.exception.RemotingException;

public class TestDefaultProducer {

    public static void main(String[] args) throws MQClientException, InterruptedException, RemotingException,
        MQBrokerException {
        DefaultMQProducer producer = new DefaultMQProducer("producer");
        producer.setNamesrvAddr("localhost:1234");

        producer.start();

        //producer.createTopic("ABC", "topic", 3);

        while (true) {
            TimeUnit.SECONDS.sleep(1);
            Message msg = new Message("topic", "{version:1.0,name:'wangjun'}".getBytes());
            producer.send(msg);
        }
    }

}

package com.ztesoft.zsmart.zmq.broker;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.ztesoft.zsmart.zmq.broker.client.ConsumerIdsChangeListener;
import com.ztesoft.zsmart.zmq.broker.client.ConsumerManager;
import com.ztesoft.zsmart.zmq.broker.client.DefaultConsumerIdsChangeListener;
import com.ztesoft.zsmart.zmq.broker.client.ProducerManager;
import com.ztesoft.zsmart.zmq.broker.topic.TopicConfigManager;
import com.ztesoft.zsmart.zmq.common.BrokerConfig;
import com.ztesoft.zsmart.zmq.common.DataVersion;
import com.ztesoft.zsmart.zmq.common.constant.LoggerName;
import com.ztesoft.zsmart.zmq.remoting.RemotingServer;
import com.ztesoft.zsmart.zmq.remoting.netty.NettyClientConfig;
import com.ztesoft.zsmart.zmq.remoting.netty.NettyServerConfig;
import com.ztesoft.zsmart.zmq.store.config.MessageStoreConfig;

/**
 * broker 主控制器 <br>
 * 
 * @author wang.jun<br>
 * @version 1.0<br>
 * @taskId <br>
 * @CreateDate 2016年3月31日 <br>
 * @since V7.3<br>
 * @see com.ztesoft.zsmart.zmq.broker <br>
 */
public class BrokerController {
    private static final Logger log = LoggerFactory.getLogger(LoggerName.BrokerLoggerName);

    private final BrokerConfig brokerConfig;

    private final NettyServerConfig nettyServerConfig;

    private final NettyClientConfig nettyClientConfig;

    private final MessageStoreConfig messageStoreConfig;

    private final ConsumerIdsChangeListener consumerIdsChangeListener;

    private final DataVersion configDataVersion = new DataVersion();

    private final ConsumerManager consumerManager;

    private final ProducerManager producerManager;

    private RemotingServer remotingServer;
    
    private TopicConfigManager topicConfigManager;

    public BrokerController(//
        final BrokerConfig brokerConfig, //
        final NettyServerConfig nettyServerConfig, //
        final NettyClientConfig nettyClientConfig, //
        final MessageStoreConfig messageStoreConfig //
    ) {
        this.brokerConfig = brokerConfig;
        this.nettyServerConfig = nettyServerConfig;
        this.nettyClientConfig = nettyClientConfig;
        this.messageStoreConfig = messageStoreConfig;

        this.topicConfigManager = new TopicConfigManager(this);
        
        this.consumerIdsChangeListener = new DefaultConsumerIdsChangeListener(this);
        this.consumerManager = new ConsumerManager(this.consumerIdsChangeListener);
        this.producerManager = new ProducerManager();

    }

    public BrokerConfig getBrokerConfig() {
        return brokerConfig;
    }

    public NettyServerConfig getNettyServerConfig() {
        return nettyServerConfig;
    }

    public NettyClientConfig getNettyClientConfig() {
        return nettyClientConfig;
    }

    public MessageStoreConfig getMessageStoreConfig() {
        return messageStoreConfig;
    }

    public DataVersion getConfigDataVersion() {
        return configDataVersion;
    }

    public ConsumerIdsChangeListener getConsumerIdsChangeListener() {
        return consumerIdsChangeListener;
    }

    public ConsumerManager getConsumerManager() {
        return consumerManager;
    }

    public ProducerManager getProducerManager() {
        return producerManager;
    }

    public RemotingServer getRemotingServer() {
        return remotingServer;
    }

    public void setRemotingServer(RemotingServer remotingServer) {
        this.remotingServer = remotingServer;
    }

}

package com.ztesoft.zsmart.zmq.broker;

import java.util.concurrent.ConcurrentHashMap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.ztesoft.zsmart.zmq.broker.client.ConsumerIdsChangeListener;
import com.ztesoft.zsmart.zmq.broker.client.ConsumerManager;
import com.ztesoft.zsmart.zmq.broker.client.DefaultConsumerIdsChangeListener;
import com.ztesoft.zsmart.zmq.broker.client.ProducerManager;
import com.ztesoft.zsmart.zmq.broker.offset.ConsumerOffsetManager;
import com.ztesoft.zsmart.zmq.broker.out.BrokerOuterAPI;
import com.ztesoft.zsmart.zmq.broker.subscription.SubscriptionGroupManager;
import com.ztesoft.zsmart.zmq.broker.topic.TopicConfigManager;
import com.ztesoft.zsmart.zmq.common.BrokerConfig;
import com.ztesoft.zsmart.zmq.common.DataVersion;
import com.ztesoft.zsmart.zmq.common.TopicConfig;
import com.ztesoft.zsmart.zmq.common.constant.LoggerName;
import com.ztesoft.zsmart.zmq.common.constant.PermName;
import com.ztesoft.zsmart.zmq.common.namesrv.RegisterBrokerResult;
import com.ztesoft.zsmart.zmq.common.protocol.body.TopicConfigSerializeWrapper;
import com.ztesoft.zsmart.zmq.remoting.RemotingServer;
import com.ztesoft.zsmart.zmq.remoting.netty.NettyClientConfig;
import com.ztesoft.zsmart.zmq.remoting.netty.NettyServerConfig;
import com.ztesoft.zsmart.zmq.store.MessageStore;
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

    private final ConsumerOffsetManager consumerOffsetManager;

    private final ConsumerManager consumerManager;

    private final ProducerManager producerManager;

    private MessageStore messageStore;

    private RemotingServer remotingServer;

    private TopicConfigManager topicConfigManager;

    private SubscriptionGroupManager subscriptionGroupManager;

    private final BrokerOuterAPI brokerOuterAPI;

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
        this.consumerOffsetManager = new ConsumerOffsetManager(this);
        this.topicConfigManager = new TopicConfigManager(this);
        this.subscriptionGroupManager = new SubscriptionGroupManager(this);

        this.brokerOuterAPI = new BrokerOuterAPI(nettyClientConfig);

        this.consumerIdsChangeListener = new DefaultConsumerIdsChangeListener(this);
        this.consumerManager = new ConsumerManager(this.consumerIdsChangeListener);
        this.producerManager = new ProducerManager();

        if (this.brokerConfig.getNamesrvAddr() != null) {
            this.brokerOuterAPI.updateNameServerAddressList(this.brokerConfig.getNamesrvAddr());
            log.info("user specfied name server address: {}", this.brokerConfig.getNamesrvAddr());
        }

    }

    public boolean initialize() {
        boolean result = true;

        result = result && this.topicConfigManager.load();
        result = result && this.consumerOffsetManager.load();
        result = result && this.subscriptionGroupManager.load();

    }

    /**
     * 注册borker topics: <br>
     * 
     * @author wang.jun<br>
     * @taskId <br>
     * @param checkOrderConfig
     * @param oneway <br>
     */
    public synchronized void registerBrokerAll(final boolean checkOrderConfig, boolean oneway) {
        TopicConfigSerializeWrapper topicConfigWrapper = this.getTopicConfigManager()
            .buildTopicConfigSerializeWrapper();
        if (!PermName.isWriteable(this.getBrokerConfig().getBrokerPermission())
            || !PermName.isReadable(this.getBrokerConfig().getBrokerPermission())) {
            ConcurrentHashMap<String, TopicConfig> topicConfigTable = new ConcurrentHashMap<String, TopicConfig>(
                topicConfigWrapper.getTopicConfigTable());

            for (TopicConfig topicConfig : topicConfigTable.values()) {
                topicConfig.setPerm(this.getBrokerConfig().getBrokerPermission());
            }

            topicConfigWrapper.setTopicConfigTable(topicConfigTable);
        }

        RegisterBrokerResult registerBrokerResult = this.brokerOuterAPI.registerBrokerAll(//
            this.brokerConfig.getBrokerClusterName(), //
            this.getBrokerAddr(),//
            this.brokerConfig.getBrokerName(), //
            this.brokerConfig.getBrokerId(),//
            this.getHAServerAddr(), //
            topicConfigWrapper,//
            this.filterServerManager.buildNewFilterServerList(),//
            oneway);

        if (registerBrokerResult != null) {

        }
    }

    private String getHAServerAddr() {
        String addr = this.brokerConfig.getBrokerIP2() + ":" + this.messageStoreConfig.getHaListenPort();
        return addr;
    }

    /**
     * 获取broker地址: <br>
     * 
     * @author wang.jun<br>
     * @taskId <br>
     * @return <br>
     */
    private String getBrokerAddr() {
        return this.brokerConfig.getBrokerIP1() + ":" + this.nettyServerConfig.getListenPort();
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

    public TopicConfigManager getTopicConfigManager() {
        return topicConfigManager;
    }

    public void setTopicConfigManager(TopicConfigManager topicConfigManager) {
        this.topicConfigManager = topicConfigManager;
    }

    public BrokerOuterAPI getBrokerOuterAPI() {
        return brokerOuterAPI;
    }

    public SubscriptionGroupManager getSubscriptionGroupManager() {
        return subscriptionGroupManager;
    }

    public void setSubscriptionGroupManager(SubscriptionGroupManager subscriptionGroupManager) {
        this.subscriptionGroupManager = subscriptionGroupManager;
    }

    public MessageStore getMessageStore() {
        return messageStore;
    }

    public void setMessageStore(MessageStore messageStore) {
        this.messageStore = messageStore;
    }

    public ConsumerOffsetManager getConsumerOffsetManager() {
        return consumerOffsetManager;
    }

}

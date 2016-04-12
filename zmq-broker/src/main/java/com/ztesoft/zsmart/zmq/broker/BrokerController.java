package com.ztesoft.zsmart.zmq.broker;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.ztesoft.zsmart.zmq.broker.client.ClientHousekeepingService;
import com.ztesoft.zsmart.zmq.broker.client.ConsumerIdsChangeListener;
import com.ztesoft.zsmart.zmq.broker.client.ConsumerManager;
import com.ztesoft.zsmart.zmq.broker.client.DefaultConsumerIdsChangeListener;
import com.ztesoft.zsmart.zmq.broker.client.ProducerManager;
import com.ztesoft.zsmart.zmq.broker.client.net.Broker2Client;
import com.ztesoft.zsmart.zmq.broker.client.rebalance.RebalanceLockManager;
import com.ztesoft.zsmart.zmq.broker.filtersrv.FilterServerManager;
import com.ztesoft.zsmart.zmq.broker.longpolling.PullRequestHoldService;
import com.ztesoft.zsmart.zmq.broker.mqtrace.ConsumeMessageHook;
import com.ztesoft.zsmart.zmq.broker.mqtrace.SendMessageHook;
import com.ztesoft.zsmart.zmq.broker.offset.ConsumerOffsetManager;
import com.ztesoft.zsmart.zmq.broker.out.BrokerOuterAPI;
import com.ztesoft.zsmart.zmq.broker.processor.AdminBrokerProcessor;
import com.ztesoft.zsmart.zmq.broker.processor.ClientManageProcessor;
import com.ztesoft.zsmart.zmq.broker.processor.EndTransactionProcessor;
import com.ztesoft.zsmart.zmq.broker.processor.PullMessageProcessor;
import com.ztesoft.zsmart.zmq.broker.processor.QueryMessageProcessor;
import com.ztesoft.zsmart.zmq.broker.processor.SendMessageProcessor;
import com.ztesoft.zsmart.zmq.broker.slave.SlaveSynchronize;
import com.ztesoft.zsmart.zmq.broker.subscription.SubscriptionGroupManager;
import com.ztesoft.zsmart.zmq.broker.topic.TopicConfigManager;
import com.ztesoft.zsmart.zmq.common.BrokerConfig;
import com.ztesoft.zsmart.zmq.common.DataVersion;
import com.ztesoft.zsmart.zmq.common.MixAll;
import com.ztesoft.zsmart.zmq.common.ThreadFactoryImpl;
import com.ztesoft.zsmart.zmq.common.TopicConfig;
import com.ztesoft.zsmart.zmq.common.UtilAll;
import com.ztesoft.zsmart.zmq.common.constant.LoggerName;
import com.ztesoft.zsmart.zmq.common.constant.PermName;
import com.ztesoft.zsmart.zmq.common.namesrv.RegisterBrokerResult;
import com.ztesoft.zsmart.zmq.common.protocol.RequestCode;
import com.ztesoft.zsmart.zmq.common.protocol.body.TopicConfigSerializeWrapper;
import com.ztesoft.zsmart.zmq.remoting.RPCHook;
import com.ztesoft.zsmart.zmq.remoting.RemotingServer;
import com.ztesoft.zsmart.zmq.remoting.netty.NettyClientConfig;
import com.ztesoft.zsmart.zmq.remoting.netty.NettyRemotingServer;
import com.ztesoft.zsmart.zmq.remoting.netty.NettyRequestProcessor;
import com.ztesoft.zsmart.zmq.remoting.netty.NettyServerConfig;
import com.ztesoft.zsmart.zmq.store.DefaultMessageStore;
import com.ztesoft.zsmart.zmq.store.MessageStore;
import com.ztesoft.zsmart.zmq.store.config.BrokerRole;
import com.ztesoft.zsmart.zmq.store.config.MessageStoreConfig;
import com.ztesoft.zsmart.zmq.store.stats.BrokerStats;
import com.ztesoft.zsmart.zmq.store.stats.BrokerStatsManager;


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

    private final DataVersion configDataVersion = new DataVersion();

    private final ConsumerOffsetManager consumerOffsetManager;

    private final ConsumerManager consumerManager;

    private final ProducerManager producerManager;

    private final ClientHousekeepingService clientHousekeepingService;

    private final PullMessageProcessor pullMessageProcessor;

    private final PullRequestHoldService pullRequestHoldService;

    private final Broker2Client broker2Client;

    private final SubscriptionGroupManager subscriptionGroupManager;

    private final ConsumerIdsChangeListener consumerIdsChangeListener;

    private final RebalanceLockManager rebalanceLockManager = new RebalanceLockManager();

    private final BrokerOuterAPI brokerOuterAPI;

    private final ScheduledExecutorService scheduledExecutorService = Executors
        .newSingleThreadScheduledExecutor(new ThreadFactoryImpl("BrokerControllerScheduledThread"));

    private final SlaveSynchronize slaveSynchronize;

    private MessageStore messageStore;

    private RemotingServer remotingServer;

    private TopicConfigManager topicConfigManager;

    private ExecutorService sendMessageExecutor;

    private ExecutorService pullMessageExecutor;

    private ExecutorService adminBrokerExecutor;

    private ExecutorService clientManageExecutor;

    private boolean updateMasterHAServerAddrPeriodically = false;

    private BrokerStats brokerStats;

    private final BlockingQueue<Runnable> sendThreadPoolQueue;

    private final BlockingQueue<Runnable> pullThreadPoolQueue;

    private final FilterServerManager filterServerManager;

    private final BrokerStatsManager brokerStatsManager;

    private InetSocketAddress storeHost;


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
        this.pullMessageProcessor = new PullMessageProcessor(this);
        this.pullRequestHoldService = new PullRequestHoldService(this);
        this.consumerIdsChangeListener = new DefaultConsumerIdsChangeListener(this);
        this.consumerManager = new ConsumerManager(this.consumerIdsChangeListener);
        this.producerManager = new ProducerManager();
        this.clientHousekeepingService = new ClientHousekeepingService(this);
        this.broker2Client = new Broker2Client(this);
        this.subscriptionGroupManager = new SubscriptionGroupManager(this);
        this.brokerOuterAPI = new BrokerOuterAPI(nettyClientConfig);
        this.filterServerManager = new FilterServerManager(this);

        if (this.brokerConfig.getNamesrvAddr() != null) {
            this.brokerOuterAPI.updateNameServerAddressList(this.brokerConfig.getNamesrvAddr());
            log.info("user specfied name server address: {}", this.brokerConfig.getNamesrvAddr());
        }

        this.slaveSynchronize = new SlaveSynchronize(this);

        this.sendThreadPoolQueue =
                new LinkedBlockingDeque<Runnable>(this.brokerConfig.getSendThreadPoolQueueCapacity());
        this.pullThreadPoolQueue =
                new LinkedBlockingDeque<Runnable>(this.brokerConfig.getPullThreadPoolQueueCapacity());

        this.brokerStatsManager = new BrokerStatsManager(this.getBrokerConfig().getBrokerClusterName());

        this.setStoreHost(new InetSocketAddress(this.brokerConfig.getBrokerIP1(),
            this.getNettyServerConfig().getListenPort()));
    }


    /**
     * broker controller 初始化 Description: <br>
     * 
     * @author wang.jun<br>
     * @taskId <br>
     * @return <br>
     */
    public boolean initialize() {
        boolean result = true;

        result = result && this.topicConfigManager.load();
        result = result && this.consumerOffsetManager.load();
        result = result && this.subscriptionGroupManager.load();

        if (result) {
            try {
                this.messageStore = new DefaultMessageStore(messageStoreConfig, brokerStatsManager);
            }
            catch (IOException e) {
                result = false;
                e.printStackTrace();
            }
        }

        result = result && this.messageStore.load();

        if (result) {
            this.remotingServer =
                    new NettyRemotingServer(this.nettyServerConfig, this.clientHousekeepingService);

            this.sendMessageExecutor = new ThreadPoolExecutor(//
                this.brokerConfig.getSendMessageThreadPoolNums(), //
                this.brokerConfig.getSendMessageThreadPoolNums(), //
                1000 * 60, //
                TimeUnit.MILLISECONDS, //
                this.sendThreadPoolQueue, //
                new ThreadFactoryImpl("SendMessageThread_"));

            this.pullMessageExecutor = new ThreadPoolExecutor(//
                this.brokerConfig.getPullThreadPoolQueueCapacity(), //
                this.brokerConfig.getPullThreadPoolQueueCapacity(), //
                1000 * 60, //
                TimeUnit.MILLISECONDS, //
                this.pullThreadPoolQueue, //
                new ThreadFactoryImpl("PullMessageThread_"));

            this.adminBrokerExecutor =
                    Executors.newFixedThreadPool(this.brokerConfig.getAdminBroderThreadPoolNums(),
                        new ThreadFactoryImpl("AdminBrokerThread_"));

            this.clientManageExecutor =
                    Executors.newFixedThreadPool(this.brokerConfig.getClientManageThreadPoolNums(),
                        new ThreadFactoryImpl("ClientManageThread_"));

            this.registerProcessor();

            this.brokerStats = new BrokerStats((DefaultMessageStore) this.messageStore);

            final long initialDelay = UtilAll.computNextMorningTimeMillis() - System.currentTimeMillis();
            final long period = 1000 * 60 * 60 * 24;
            this.scheduledExecutorService.scheduleAtFixedRate(new Runnable() {

                @Override
                public void run() {
                    try {
                        BrokerController.this.getBrokerStats().record();
                    }
                    catch (Exception e) {
                        log.error("schedule record error.", e);
                    }

                }
            }, initialDelay, period, TimeUnit.MILLISECONDS);

            this.scheduledExecutorService.scheduleAtFixedRate(new Runnable() {

                @Override
                public void run() {
                    try {
                        BrokerController.this.consumerOffsetManager.persist();
                    }
                    catch (Exception e) {
                        log.error("schedule persist consumerOffset error.", e);

                    }
                }
            }, 1000 * 10, this.getBrokerConfig().getFlushConsumerOffsetInterval(), TimeUnit.MILLISECONDS);

            this.scheduledExecutorService.scheduleAtFixedRate(new Runnable() {
                @Override
                public void run() {
                    try {
                        BrokerController.this.consumerOffsetManager.scanUnsubscribedTopic();
                    }
                    catch (Exception e) {
                        log.error("schedule scanUnsubscribedTopic error.", e);
                    }
                }
            }, 10, 60, TimeUnit.MINUTES);

            if (this.brokerConfig.getNamesrvAddr() != null) {
                this.brokerOuterAPI.updateNameServerAddressList(this.brokerConfig.getNamesrvAddr());
            }
            else if (this.brokerConfig.isFetchNamesrvAddrByAddressServer()) {
                this.scheduledExecutorService.scheduleAtFixedRate(new Runnable() {

                    @Override
                    public void run() {
                        try {
                            BrokerController.this.brokerOuterAPI.fetchNameServerAddr();
                        }
                        catch (Exception e) {
                            log.error("ScheduledTask fetchNameServerAddr exception", e);
                        }
                    }
                }, 1000 * 10, 1000 * 60 * 2, TimeUnit.MILLISECONDS);
            }

            if (BrokerRole.SLAVE == this.messageStoreConfig.getBrokerRole()) {
                if (this.messageStoreConfig.getHaMasterAddress() != null
                        && this.messageStoreConfig.getHaMasterAddress().length() >= 6) {
                    this.messageStore.updateHaMasterAddress(this.messageStoreConfig.getHaMasterAddress());
                    this.updateMasterHAServerAddrPeriodically = false;
                }
                else {
                    this.updateMasterHAServerAddrPeriodically = true;
                }

                this.scheduledExecutorService.scheduleAtFixedRate(new Runnable() {

                    @Override
                    public void run() {
                        try {
                            BrokerController.this.getSlaveSynchronize().syncAll();
                        }
                        catch (Exception e) {
                            log.error("ScheduledTask syncAll slave exception", e);
                        }

                    }
                }, 1000 * 10, 1000 * 60, TimeUnit.MILLISECONDS);

            }
            else {
                this.scheduledExecutorService.scheduleAtFixedRate(new Runnable() {

                    @Override
                    public void run() {
                        try {
                            BrokerController.this.printMasterAndSlaveDiff();
                        }
                        catch (Exception e) {
                            log.error("schedule printMasterAndSlaveDiff error.", e);
                        }
                    }
                }, 1000 * 10, 1000 * 60, TimeUnit.MILLISECONDS);
            }
        }

        return result;
    }


    /**
     * 注册remoting 处理器 Description: <br>
     * 
     * @author wang.jun<br>
     * @taskId <br>
     *         <br>
     */
    private void registerProcessor() {
        /**
         * SendMessageProcessor
         */
        SendMessageProcessor sendProcessor = new SendMessageProcessor(this);
        sendProcessor.registerSendMessageHook(sendMessageHookList);

        this.remotingServer.registerProcessor(RequestCode.SEND_MESSAGE, sendProcessor,
            this.sendMessageExecutor);
        this.remotingServer.registerProcessor(RequestCode.SEND_MESSAGE_V2, sendProcessor,
            this.sendMessageExecutor);
        this.remotingServer.registerProcessor(RequestCode.CONSUMER_SEND_MSG_BACK, sendProcessor,
            this.sendMessageExecutor);

        /**
         * PullMessageProcessor
         */
        this.remotingServer.registerProcessor(RequestCode.PULL_MESSAGE, this.pullMessageProcessor,
            this.pullMessageExecutor);
        this.pullMessageProcessor.registerConsumeMessageHook(consumeMessageHookList);

        /**
         * QueryMessageProcessor
         */
        NettyRequestProcessor queryProcessor = new QueryMessageProcessor(this);
        this.remotingServer.registerProcessor(RequestCode.QUERY_MESSAGE, queryProcessor,
            this.pullMessageExecutor);
        this.remotingServer.registerProcessor(RequestCode.VIEW_MESSAGE_BY_ID, queryProcessor,
            this.pullMessageExecutor);

        /**
         * ClientManageProcessor
         */
        ClientManageProcessor clientProcessor = new ClientManageProcessor(this);
        clientProcessor.registerConsumeMessageHook(this.consumeMessageHookList);
        this.remotingServer.registerProcessor(RequestCode.HEART_BEAT, clientProcessor,
            this.clientManageExecutor);
        this.remotingServer.registerProcessor(RequestCode.UNREGISTER_CLIENT, clientProcessor,
            this.clientManageExecutor);
        this.remotingServer.registerProcessor(RequestCode.GET_CONSUMER_LIST_BY_GROUP, clientProcessor,
            this.clientManageExecutor);

        /**
         * Offset存储更新转移到ClientProcessor处理
         */
        this.remotingServer.registerProcessor(RequestCode.UPDATE_CONSUMER_OFFSET, clientProcessor,
            this.clientManageExecutor);
        this.remotingServer.registerProcessor(RequestCode.QUERY_CONSUMER_OFFSET, clientProcessor,
            this.clientManageExecutor);

        /**
         * EndTransactionProcessor
         */
        this.remotingServer.registerProcessor(RequestCode.END_TRANSACTION, new EndTransactionProcessor(this),
            this.sendMessageExecutor);

        /**
         * Default
         */
        AdminBrokerProcessor adminProcessor = new AdminBrokerProcessor(this);
        this.remotingServer.registerDefaultProcessor(adminProcessor, this.adminBrokerExecutor);

    }


    public void start() throws Exception {
        if (this.messageStore != null) {
            this.messageStore.start();
        }

        if (this.remotingServer != null) {
            this.remotingServer.start();
        }

        if (this.brokerOuterAPI != null) {
            this.brokerOuterAPI.start();
        }

        if (this.pullRequestHoldService != null) {
            this.pullRequestHoldService.start();
        }

        if (this.clientHousekeepingService != null) {
            this.clientHousekeepingService.start();
        }

        if (this.filterServerManager != null) {
            this.filterServerManager.start();
        }

        this.registerBrokerAll(true, false);

        this.scheduledExecutorService.scheduleAtFixedRate(new Runnable() {

            @Override
            public void run() {
                try {
                    BrokerController.this.registerBrokerAll(true, false);
                }
                catch (Exception e) {
                    log.error("registerBrokerAll Exception", e);
                }
            }
        }, 1000 * 10, 1000 * 30, TimeUnit.MILLISECONDS);

        if (this.brokerStatsManager != null) {
            this.brokerStatsManager.start();
        }

        this.addDeleteTopicTask();
    }


    public void addDeleteTopicTask() {
        this.scheduledExecutorService.schedule(new Runnable() {

            @Override
            public void run() {
                int removedTopicCnt = BrokerController.this.messageStore.cleanUnusedTopic(
                    BrokerController.this.getTopicConfigManager().getTopicConfigTable().keySet());
                log.info("addDeleteTopicTask removed topic count {}", removedTopicCnt);

            }
        }, 5, TimeUnit.MINUTES);

    }


    public void shutdown() {
        if (this.brokerStatsManager != null) {
            this.brokerStatsManager.shutdown();
        }

        if (this.clientHousekeepingService != null) {
            this.clientHousekeepingService.shutdown();
        }

        if (this.pullRequestHoldService != null) {
            this.pullRequestHoldService.shutdown();
        }

        if (this.remotingServer != null) {
            this.remotingServer.shutdown();
        }

        if (this.messageStore != null) {
            this.messageStore.shutdown();
        }

        this.scheduledExecutorService.shutdown();
        try {
            this.scheduledExecutorService.awaitTermination(5000, TimeUnit.MILLISECONDS);
        }
        catch (InterruptedException e) {
        }

        this.unregisterBrokerAll();

        if (this.sendMessageExecutor != null) {
            this.sendMessageExecutor.shutdown();
        }

        if (this.pullMessageExecutor != null) {
            this.pullMessageExecutor.shutdown();
        }

        if (this.adminBrokerExecutor != null) {
            this.adminBrokerExecutor.shutdown();
        }

        if (this.brokerOuterAPI != null) {
            this.brokerOuterAPI.shutdown();
        }

        this.consumerOffsetManager.persist();

        if (this.filterServerManager != null) {
            this.filterServerManager.shutdown();
        }
    }


    private void unregisterBrokerAll() {
        this.brokerOuterAPI.unregisterBrokerAll(//
            this.brokerConfig.getBrokerClusterName(), //
            this.getBrokerAddr(), //
            this.brokerConfig.getBrokerName(), //
            this.brokerConfig.getBrokerId());

    }


    /**
     * 注册borker topics: <br>
     * 
     * @author wang.jun<br>
     * @taskId <br>
     * @param checkOrderConfig
     * @param oneway
     *            <br>
     */
    public synchronized void registerBrokerAll(final boolean checkOrderConfig, boolean oneway) {
        TopicConfigSerializeWrapper topicConfigWrapper =
                this.getTopicConfigManager().buildTopicConfigSerializeWrapper();
        if (!PermName.isWriteable(this.getBrokerConfig().getBrokerPermission())
                || !PermName.isReadable(this.getBrokerConfig().getBrokerPermission())) {
            ConcurrentHashMap<String, TopicConfig> topicConfigTable =
                    new ConcurrentHashMap<String, TopicConfig>(topicConfigWrapper.getTopicConfigTable());

            for (TopicConfig topicConfig : topicConfigTable.values()) {
                topicConfig.setPerm(this.getBrokerConfig().getBrokerPermission());
            }

            topicConfigWrapper.setTopicConfigTable(topicConfigTable);
        }

        RegisterBrokerResult registerBrokerResult = this.brokerOuterAPI.registerBrokerAll(//
            this.brokerConfig.getBrokerClusterName(), //
            this.getBrokerAddr(), //
            this.brokerConfig.getBrokerName(), //
            this.brokerConfig.getBrokerId(), //
            this.getHAServerAddr(), //
            topicConfigWrapper, //
            this.filterServerManager.buildNewFilterServerList(), //
            oneway);

        if (registerBrokerResult != null) {
            if (this.updateMasterHAServerAddrPeriodically && registerBrokerResult.getHaServerAddr() != null) {
                this.messageStore.updateHaMasterAddress(registerBrokerResult.getHaServerAddr());
            }

            this.slaveSynchronize.setMasterAddr(registerBrokerResult.getMasterAddr());

            if (checkOrderConfig) {
                this.getTopicConfigManager().updateOrderTopicConfig(registerBrokerResult.getKvTable());
            }
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
    public String getBrokerAddr() {
        return this.brokerConfig.getBrokerIP1() + ":" + this.nettyServerConfig.getListenPort();
    }


    /**
     * 更新broker配置 Description: <br>
     * 
     * @author wang.jun<br>
     * @taskId <br>
     * @param properties
     *            <br>
     */
    public void updateAllConfig(Properties properties) {
        MixAll.properties2Object(properties, this.brokerConfig);
        MixAll.properties2Object(properties, this.nettyServerConfig);
        MixAll.properties2Object(properties, this.nettyClientConfig);
        MixAll.properties2Object(properties, this.messageStoreConfig);
        this.configDataVersion.nextVersion();
        this.flushAllConfig();

    }


    /**
     * Description: <br>
     * 
     * @author wang.jun<br>
     * @taskId <br>
     *         <br>
     */
    private void flushAllConfig() {
        String allConfig = this.encodeAllConfig();
        try {
            MixAll.string2File(allConfig, BrokerPathConfigHelper.getBrokerConfigPath());
            log.info("flush broker config, {} OK", BrokerPathConfigHelper.getBrokerConfigPath());
        }
        catch (IOException e) {
            log.info("flush broker config Exception, " + BrokerPathConfigHelper.getBrokerConfigPath(), e);
        }

    }


    public String encodeAllConfig() {
        StringBuilder sb = new StringBuilder();
        {
            Properties properties = MixAll.object2Properties(this.brokerConfig);
            if (properties != null) {
                sb.append(MixAll.properties2String(properties));
            }
            else {
                log.error("encodeAllConfig object2Properties error");
            }
        }

        {
            Properties properties = MixAll.object2Properties(this.messageStoreConfig);
            if (properties != null) {
                sb.append(MixAll.properties2String(properties));
            }
            else {
                log.error("encodeAllConfig object2Properties error");
            }
        }

        {
            Properties properties = MixAll.object2Properties(this.nettyServerConfig);
            if (properties != null) {
                sb.append(MixAll.properties2String(properties));
            }
            else {
                log.error("encodeAllConfig object2Properties error");
            }
        }

        {
            Properties properties = MixAll.object2Properties(this.nettyClientConfig);
            if (properties != null) {
                sb.append(MixAll.properties2String(properties));
            }
            else {
                log.error("encodeAllConfig object2Properties error");
            }
        }
        return sb.toString();
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


    public String getConfigDataVersion() {
        return configDataVersion.toJson();
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


    public MessageStore getMessageStore() {
        return messageStore;
    }


    public void setMessageStore(MessageStore messageStore) {
        this.messageStore = messageStore;
    }


    public ConsumerOffsetManager getConsumerOffsetManager() {
        return consumerOffsetManager;
    }


    public ExecutorService getSendMessageExecutor() {
        return sendMessageExecutor;
    }


    public void setSendMessageExecutor(ExecutorService sendMessageExecutor) {
        this.sendMessageExecutor = sendMessageExecutor;
    }


    public ExecutorService getPullMessageExecutor() {
        return pullMessageExecutor;
    }


    public void setPullMessageExecutor(ExecutorService pullMessageExecutor) {
        this.pullMessageExecutor = pullMessageExecutor;
    }


    public ExecutorService getAdminBrokerExecutor() {
        return adminBrokerExecutor;
    }


    public void setAdminBrokerExecutor(ExecutorService adminBrokerExecutor) {
        this.adminBrokerExecutor = adminBrokerExecutor;
    }


    public ExecutorService getClientManageExecutor() {
        return clientManageExecutor;
    }


    public void setClientManageExecutor(ExecutorService clientManageExecutor) {
        this.clientManageExecutor = clientManageExecutor;
    }


    public boolean isUpdateMasterHAServerAddrPeriodically() {
        return updateMasterHAServerAddrPeriodically;
    }


    public void setUpdateMasterHAServerAddrPeriodically(boolean updateMasterHAServerAddrPeriodically) {
        this.updateMasterHAServerAddrPeriodically = updateMasterHAServerAddrPeriodically;
    }


    public BrokerStats getBrokerStats() {
        return brokerStats;
    }


    public void setBrokerStats(BrokerStats brokerStats) {
        this.brokerStats = brokerStats;
    }


    public ClientHousekeepingService getClientHousekeepingService() {
        return clientHousekeepingService;
    }


    public PullMessageProcessor getPullMessageProcessor() {
        return pullMessageProcessor;
    }


    public PullRequestHoldService getPullRequestHoldService() {
        return pullRequestHoldService;
    }


    public Broker2Client getBroker2Client() {
        return broker2Client;
    }


    public RebalanceLockManager getRebalanceLockManager() {
        return rebalanceLockManager;
    }


    public ScheduledExecutorService getScheduledExecutorService() {
        return scheduledExecutorService;
    }


    public SlaveSynchronize getSlaveSynchronize() {
        return slaveSynchronize;
    }


    public BlockingQueue<Runnable> getSendThreadPoolQueue() {
        return sendThreadPoolQueue;
    }


    public BlockingQueue<Runnable> getPullThreadPoolQueue() {
        return pullThreadPoolQueue;
    }


    public FilterServerManager getFilterServerManager() {
        return filterServerManager;
    }


    public BrokerStatsManager getBrokerStatsManager() {
        return brokerStatsManager;
    }


    public InetSocketAddress getStoreHost() {
        return storeHost;
    }


    public void setStoreHost(InetSocketAddress storeHost) {
        this.storeHost = storeHost;
    }


    private void printMasterAndSlaveDiff() {
        long diff = this.messageStore.slaveFallBehindMuch();

        // XXX: warn and notify me
        log.info("slave fall behind master, how much, {} bytes", diff);
    }

    private final List<SendMessageHook> sendMessageHookList = new ArrayList<SendMessageHook>();


    public void registerSendMessageHook(final SendMessageHook hook) {
        this.sendMessageHookList.add(hook);
        log.info("register SendMessageHook Hook, {}", hook.hookName());
    }

    private final List<ConsumeMessageHook> consumeMessageHookList = new ArrayList<ConsumeMessageHook>();


    public void registerConsumeMessageHook(final ConsumeMessageHook hook) {
        this.consumeMessageHookList.add(hook);
        log.info("register ConsumeMessageHook Hook, {}", hook.hookName());
    }


    public void registerServerRPCHook(RPCHook rpcHook) {
        getRemotingServer().registerRPCHook(rpcHook);
    }


    public void registerClientRPCHook(RPCHook rpcHook) {
        this.getBrokerOuterAPI().registerRPCHook(rpcHook);
    }

}

package com.ztesoft.zsmart.zmq.client.impl.factory;

import java.net.DatagramSocket;
import java.util.HashMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import org.slf4j.Logger;

import com.ztesoft.zsmart.zmq.client.ClientConfig;
import com.ztesoft.zsmart.zmq.client.admin.MQAdminExtInner;
import com.ztesoft.zsmart.zmq.client.exception.MQClientException;
import com.ztesoft.zsmart.zmq.client.impl.ClientRemotingProcessor;
import com.ztesoft.zsmart.zmq.client.impl.MQAdminImpl;
import com.ztesoft.zsmart.zmq.client.impl.MQClientAPIImpl;
import com.ztesoft.zsmart.zmq.client.impl.consumer.MQConsumerInner;
import com.ztesoft.zsmart.zmq.client.impl.consumer.PullMessageService;
import com.ztesoft.zsmart.zmq.client.impl.consumer.RebalanceService;
import com.ztesoft.zsmart.zmq.client.impl.producer.MQProducerInner;
import com.ztesoft.zsmart.zmq.client.log.ClientLogger;
import com.ztesoft.zsmart.zmq.client.producer.DefaultMQProducer;
import com.ztesoft.zsmart.zmq.client.stat.ConsumerStatsManager;
import com.ztesoft.zsmart.zmq.common.MQVersion;
import com.ztesoft.zsmart.zmq.common.MixAll;
import com.ztesoft.zsmart.zmq.common.ServiceState;
import com.ztesoft.zsmart.zmq.common.conflict.PackageConflictDetect;
import com.ztesoft.zsmart.zmq.common.protocol.route.TopicRouteData;
import com.ztesoft.zsmart.zmq.remoting.RPCHook;
import com.ztesoft.zsmart.zmq.remoting.netty.NettyClientConfig;

public class MQClientInstance {
    private final static long LockTimeoutMillis = 3000;

    private final Logger log = ClientLogger.getLog();

    private final ClientConfig clientConfig;

    private final int instanceIndex;

    private final String clientId;

    private final long bootTimestamp = System.currentTimeMillis();

    private final ConcurrentHashMap<String/* group */, MQProducerInner> producerTable = new ConcurrentHashMap<String, MQProducerInner>();

    private final ConcurrentHashMap<String/* group */, MQConsumerInner> consumerTable = new ConcurrentHashMap<String, MQConsumerInner>();

    private final ConcurrentHashMap<String/* group */, MQAdminExtInner> adminExtTable = new ConcurrentHashMap<String, MQAdminExtInner>();
    
    private final NettyClientConfig nettyClientConfig;
    private final MQClientAPIImpl mQClientAPIImpl;
    private final MQAdminImpl mQAdminImpl;
    private final ConcurrentHashMap<String/* Topic */, TopicRouteData> topicRouteTable =
        new ConcurrentHashMap<String, TopicRouteData>();
    private final Lock lockNamesrv = new ReentrantLock();
    private final Lock lockHeartbeat = new ReentrantLock();
    private final ConcurrentHashMap<String/* Broker Name */, HashMap<Long/* brokerId */, String/* address */>> brokerAddrTable =
        new ConcurrentHashMap<String, HashMap<Long, String>>();
    private final ScheduledExecutorService scheduledExecutorService = Executors
        .newSingleThreadScheduledExecutor(new ThreadFactory() {
            @Override
            public Thread newThread(Runnable r) {
                return new Thread(r, "MQClientFactoryScheduledThread");
            }
        });
    private final ClientRemotingProcessor clientRemotingProcessor;
    private final PullMessageService pullMessageService;
    private final RebalanceService rebalanceService;
    private final DefaultMQProducer defaultMQProducer;
    private ServiceState serviceState = ServiceState.CREATE_JUST;
    private DatagramSocket datagramSocket;
    private final ConsumerStatsManager consumerStatsManager;

    public MQClientInstance(ClientConfig clientConfig, int instanceIndex, String clientId, RPCHook rpcHook){
        this.clientConfig = clientConfig;
        this.instanceIndex = instanceIndex;
        this.nettyClientConfig = new NettyClientConfig();
        this.nettyClientConfig.setClientCallbackExecutorThread(clientConfig
            .getClientCallbackExecutorThreads());
        this.clientRemotingProcessor = new ClientRemotingProcessor(this);
        this.mQClientAPIImpl =
            new MQClientAPIImpl(this.nettyClientConfig, this.clientRemotingProcessor, rpcHook);
        if (this.clientConfig.getNamesrvAddr() != null) {
            this.mQClientAPIImpl.updateNameServerAddressList(this.clientConfig.getNamesrvAddr());
            log.info("user specified name server address: {}", this.clientConfig.getNamesrvAddr());
        }
        
        this.clientId = clientId;

        this.mQAdminImpl = new MQAdminImpl(this);

        this.pullMessageService = new PullMessageService(this);

        this.rebalanceService = new RebalanceService(this);

        this.defaultMQProducer = new DefaultMQProducer(MixAll.CLIENT_INNER_PRODUCER_GROUP);
        this.defaultMQProducer.resetClientConfig(clientConfig);

        this.consumerStatsManager = new ConsumerStatsManager(this.scheduledExecutorService);

        log.info("created a new client Instance, FactoryIndex: {} ClinetID: {} {} {}",//
                this.instanceIndex, //
                this.clientId, //
                this.clientConfig, //
                MQVersion.getVersionDesc(MQVersion.CurrentVersion));


    }
    
    public MQClientInstance(ClientConfig clientConfig, int instanceIndex, String clientId) {
        this(clientConfig, instanceIndex, clientId, null);
    }
    
    public void start() throws MQClientException {
        PackageConflictDetect.detectFastjson();

        synchronized (this) {
            switch (this.serviceState) {
                case CREATE_JUST:
                    this.serviceState = ServiceState.START_FAILED;
                    //If not specified,looking address from name server
                    if (null == this.clientConfig.getNamesrvAddr()) {
                        this.clientConfig.setNamesrvAddr(this.mQClientAPIImpl.fetchNameServerAddr());
                    }
                    //Start request-response channel
                    this.mQClientAPIImpl.start();
                    //Start various schedule tasks
                    this.startScheduledTask();
                    //Start pull service
                    this.pullMessageService.start();
                    //Start rebalance service
                    this.rebalanceService.start();
                    //Start push service
                    this.defaultMQProducer.getDefaultMQProducerImpl().start(false);
                    log.info("the client factory [{}] start OK", this.clientId);
                    this.serviceState = ServiceState.RUNNING;
                    break;
                case RUNNING:
                    break;
                case SHUTDOWN_ALREADY:
                    break;
                case START_FAILED:
                    throw new MQClientException("The Factory object[" + this.getClientId()
                            + "] has been created before, and failed.", null);
                default:
                    break;
            }
        }
    }
    
    private void startScheduledTask() {
        if (null == this.clientConfig.getNamesrvAddr()) {
            this.scheduledExecutorService.scheduleAtFixedRate(new Runnable() {

                @Override
                public void run() {
                    try {
                        MQClientInstance.this.mQClientAPIImpl.fetchNameServerAddr();
                    } catch (Exception e) {
                        log.error("ScheduledTask fetchNameServerAddr exception", e);
                    }
                }
            }, 1000 * 10, 1000 * 60 * 2, TimeUnit.MILLISECONDS);
        }

        this.scheduledExecutorService.scheduleAtFixedRate(new Runnable() {

            @Override
            public void run() {
                try {
                    MQClientInstance.this.updateTopicRouteInfoFromNameServer();
                } catch (Exception e) {
                    log.error("ScheduledTask updateTopicRouteInfoFromNameServer exception", e);
                }
            }
        }, 10, this.clientConfig.getPollNameServerInteval(), TimeUnit.MILLISECONDS);

        this.scheduledExecutorService.scheduleAtFixedRate(new Runnable() {

            @Override
            public void run() {
                try {
                    MQClientInstance.this.cleanOfflineBroker();
                    MQClientInstance.this.sendHeartbeatToAllBrokerWithLock();
                } catch (Exception e) {
                    log.error("ScheduledTask sendHeartbeatToAllBroker exception", e);
                }
            }
        }, 1000, this.clientConfig.getHeartbeatBrokerInterval(), TimeUnit.MILLISECONDS);

        this.scheduledExecutorService.scheduleAtFixedRate(new Runnable() {

            @Override
            public void run() {
                try {
                    MQClientInstance.this.persistAllConsumerOffset();
                } catch (Exception e) {
                    log.error("ScheduledTask persistAllConsumerOffset exception", e);
                }
            }
        }, 1000 * 10, this.clientConfig.getPersistConsumerOffsetInterval(), TimeUnit.MILLISECONDS);

        this.scheduledExecutorService.scheduleAtFixedRate(new Runnable() {

            @Override
            public void run() {
                try {
                    MQClientInstance.this.adjustThreadPool();
                } catch (Exception e) {
                    log.error("ScheduledTask adjustThreadPool exception", e);
                }
            }
        }, 1, 1, TimeUnit.MINUTES);
    }
    
    

}

package com.ztesoft.zsmart.zmq.namesrv;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.ztesoft.zsmart.zmq.common.ThreadFactoryImpl;
import com.ztesoft.zsmart.zmq.common.constant.LoggerName;
import com.ztesoft.zsmart.zmq.common.namesrv.NamesrvConfig;
import com.ztesoft.zsmart.zmq.namesrv.kvconfig.KVConfigManager;
import com.ztesoft.zsmart.zmq.namesrv.routeinfo.BrokerHouseKeepingService;
import com.ztesoft.zsmart.zmq.namesrv.routeinfo.RouteInfoManager;
import com.ztesoft.zsmart.zmq.remoting.RemotingServer;
import com.ztesoft.zsmart.zmq.remoting.netty.NettyRemotingServer;
import com.ztesoft.zsmart.zmq.remoting.netty.NettyServerConfig;

/**
 * Name Server 服务控制 <br>
 * 
 * @author wang.jun<br>
 * @version 1.0<br>
 * @taskId <br>
 * @CreateDate 2016年3月21日 <br>
 * @since V7.3<br>
 * @see com.ztesoft.zsmart.zmq.namesrv <br>
 */
public class NamesrvController {
    private static final Logger log = LoggerFactory.getLogger(LoggerName.NamesrvLoggerName);

    // Name Server 配置
    private final NamesrvConfig namesrvConfig;

    // 通信层配置
    private final NettyServerConfig nettyServerConfig;

    // 服务端通信对象
    private RemotingServer remotingServer;

    // 接收Broker连接事件
    private BrokerHouseKeepingService brokerHousekeepingService;

    // 服务端网络请求处理线程池
    private ExecutorService remotingExecutor;

    // 定时任务
    private final ScheduledExecutorService scheduledExecutorService = Executors
        .newSingleThreadScheduledExecutor(new ThreadFactoryImpl("NSScheduledThread"));

    /**
     * 核心数据结构
     */
    private final KVConfigManager kvConfigManager;

    private final RouteInfoManager routeInfoManager;

    public NamesrvController(NamesrvConfig namesrvConfig, NettyServerConfig nettyServerConfig) {
        this.namesrvConfig = namesrvConfig;
        this.nettyServerConfig = nettyServerConfig;
        this.kvConfigManager = new KVConfigManager(this);
        this.routeInfoManager = new RouteInfoManager();
        this.brokerHousekeepingService = new BrokerHouseKeepingService(this);
    }

    public boolean initialize() {
        // // 加载KV配置
        this.kvConfigManager.load();
        // 初始化通信层
        this.remotingServer = new NettyRemotingServer(this.nettyServerConfig);

        // 初始化线程池
        this.remotingExecutor = Executors.newFixedThreadPool(nettyServerConfig.getServerWorkThreads(),
            new ThreadFactoryImpl("RemotingExecutorThread_"));

        this.registerProcessor();

        // 增加定时任务
        this.scheduledExecutorService.scheduleAtFixedRate(new Runnable() {

            @Override
            public void run() {
                NamesrvController.this.routeInfoManager.scanNotActiveBroker();

            }
        }, 5, 10, TimeUnit.SECONDS);

        this.scheduledExecutorService.scheduleAtFixedRate(new Runnable() {

            @Override
            public void run() {
                NamesrvController.this.kvConfigManager.printAllPeriodically();
            }
        }, 1, 10, TimeUnit.MINUTES);
        
        return true;
    }
    
    public void registerProcessor(){
        //this.remotingServer.registerDefaultProcessor(new defaultr, executor);
    }
    
    public void start(){
        this.remotingServer.start();
    }
    
    public void shutdown(){
        this.remotingServer.shutdown();
        this.remotingExecutor.shutdown();
        this.scheduledExecutorService.shutdown();
    }
    
    public NamesrvConfig getNamesrvConfig() {
        return namesrvConfig;
    }


    public NettyServerConfig getNettyServerConfig() {
        return nettyServerConfig;
    }


    public KVConfigManager getKvConfigManager() {
        return kvConfigManager;
    }


    public RouteInfoManager getRouteInfoManager() {
        return routeInfoManager;
    }


    public RemotingServer getRemotingServer() {
        return remotingServer;
    }


    public void setRemotingServer(RemotingServer remotingServer) {
        this.remotingServer = remotingServer;
    }
}
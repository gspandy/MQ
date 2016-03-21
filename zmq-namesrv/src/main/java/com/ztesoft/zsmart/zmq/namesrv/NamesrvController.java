package com.ztesoft.zsmart.zmq.namesrv;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.ztesoft.zsmart.zmq.common.ThreadFactoryImpl;
import com.ztesoft.zsmart.zmq.common.constant.LoggerName;
import com.ztesoft.zsmart.zmq.common.namesrv.NamesrvConfig;
import com.ztesoft.zsmart.zmq.namesrv.routeinfo.BrokerHouseKeepingService;
import com.ztesoft.zsmart.zmq.namesrv.routeinfo.RouteInfoManager;
import com.ztesoft.zsmart.zmq.remoting.RemotingServer;
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
    private BrokerHouseKeepingService brokerHouseKeepingService;

    // 服务端网络请求处理线程池
    private ExecutorService remotingService;

    // 定时任务
    private final ScheduledExecutorService scheduledExecutorService = Executors
        .newSingleThreadScheduledExecutor(new ThreadFactoryImpl("NSScheduledThread"));

    /**
     * 核心数据结构
     */
    private final RouteInfoManager routeInfoManager;

    public NamesrvController(NamesrvConfig namesrvConfig, NettyServerConfig nettyServerConfig) {
        this.namesrvConfig = namesrvConfig;
        this.nettyServerConfig = nettyServerConfig;
        this.kvConfigManager = new KVConfigManager(this);
        this.routeInfoManager = new RouteInfoManager();
        this.brokerHousekeepingService = new BrokerHousekeepingService(this);
    }

}

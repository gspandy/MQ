package com.ztesoft.zsmart.zmq.broker.filtersrv;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.ztesoft.zsmart.zmq.broker.BrokerController;
import com.ztesoft.zsmart.zmq.common.ThreadFactoryImpl;
import com.ztesoft.zsmart.zmq.common.constant.LoggerName;

import io.netty.channel.Channel;


public class FilterServerManager {
    private static final Logger log = LoggerFactory.getLogger(LoggerName.BrokerLoggerName);
    // Filter Server最大空闲时间
    public static final long FilterServerMaxIdleTimeMills = 30000;

    private final ConcurrentHashMap<Channel /* 注册连接 */, FilterServerInfo /* filterServer监听端口 */> filterServerTable =
            new ConcurrentHashMap<Channel, FilterServerInfo>();

    private final BrokerController brokerController;

    private ScheduledExecutorService scheduledExecutorService = Executors
        .newSingleThreadScheduledExecutor(new ThreadFactoryImpl("FilterServerManagerScheduledThread"));


    public FilterServerManager(final BrokerController brokerController) {
        this.brokerController = brokerController;
    }


    public void start() {
        // 定时检查Filter Server个数 数量不符合 则创建
        this.scheduledExecutorService.scheduleAtFixedRate(new Runnable() {

            @Override
            public void run() {
                try {
                    FilterServerManager.this.createFilterServer();
                }
                catch (Exception e) {
                    log.error("", e);
                }
            }
        }, 1000 * 5, 1000 * 30, TimeUnit.MILLISECONDS);

    }
    
    public void shutdown(){
        this.scheduledExecutorService.shutdown();
    }
    
    private String buildStartCommand(){
        String config = "";
        if(broker)
    }
    
    

    class FilterServerInfo {
        private String filterServerAddr;
        private long lastUpdateTimestamp;


        public String getFilterServerAddr() {
            return filterServerAddr;
        }


        public void setFilterServerAddr(String filterServerAddr) {
            this.filterServerAddr = filterServerAddr;
        }


        public long getLastUpdateTimestamp() {
            return lastUpdateTimestamp;
        }


        public void setLastUpdateTimestamp(long lastUpdateTimestamp) {
            this.lastUpdateTimestamp = lastUpdateTimestamp;
        }
    }

}

package com.ztesoft.zsmart.zmq.common;

import java.net.InetAddress;
import java.net.UnknownHostException;

import com.ztesoft.zsmart.zmq.common.annotation.ImportantField;
import com.ztesoft.zsmart.zmq.common.constant.PermName;
import com.ztesoft.zsmart.zmq.remoting.common.RemotingUtil;

/**
 * 服务器配置 <br>
 * 
 * @author wang.jun<br>
 * @version 1.0<br>
 * @taskId <br>
 * @CreateDate 2016年3月21日 <br>
 * @since V7.3<br>
 * @see com.ztesoft.zsmart.zmq.common <br>
 */
public class BrokerConfig {
    private String zmqHome = System.getProperty(MixAll.ZMQ_HOME_PROPERTY, System.getenv(MixAll.ZMQ_HOME_ENV));

    @ImportantField
    private String namesrvAddr = System.getProperty(MixAll.NAMESRV_ADDR_PROPERTY,
        System.getenv(MixAll.NAMESRV_ADDR_ENV));

    @ImportantField
    private String brokerIP1 = RemotingUtil.getLocalAddress();

    private String brokerIP2 = RemotingUtil.getLocalAddress();

    @ImportantField
    private String brokerName = localHostName();

    @ImportantField
    private String brokerClusterName = "DefaultCluster";

    @ImportantField
    private long brokerId = MixAll.MASTER_ID;

    private int brokerPermission = PermName.PERM_READ | PermName.PERM_WRITE;

    private int defaultTopicQueueNums = 8;

    // 自动创建topic功能是否开启（线上建议关闭）
    @ImportantField
    private boolean autoCreateTopicEnable = true;

    // 自动创建以集群名字命名的Topic功能是否开启
    private boolean clusterTopicEnable = true;

    // 自动创建以服务器名字命名的Topic功能是否开启
    private boolean brokerTopicEnable = true;

    // 自动创建订阅组功能是否开启（线上建议关闭）
    @ImportantField
    private boolean autoCreateSubscriptionGroup = true;

    private int sendMessageThreadPoolNums = 16 + Runtime.getRuntime().availableProcessors() * 4;

    private int pullMessageThreadPoolNums = 16 + Runtime.getRuntime().availableProcessors() * 2;

    private int adminBroderThreadPoolNums = 16;

    private int clientManageThreadPoolNums = 16;

    private int flushConsumerOffsetInterval = 1000 * 5;

    private int flushConsumerOffsetHistoryInterval = 1000 * 60;

    // 是否拒绝接收事务消息
    @ImportantField
    private boolean rejectTransactionMessage = false;

    // 是否从地址服务器寻找Name Server地址，正式发布后，默认值为false
    @ImportantField
    private boolean fetchNamesrvAddrByAddressServer = false;

    // 发送消息对应的线程池阻塞队列size
    private int sendThreadPoolQueueCapacity = 100000;

    // 订阅消息对的线程池阻塞size
    private int pullThreadPoolQueueCapacity = 100000;

    // 过滤服务器数量
    private int filterServerNums = 0;

    // Consumer订阅消息时，Broker是否开启长轮询
    private boolean longPoolingEnable = true;

    // 如果是短轮询，服务器挂起时间
    private long shortPollingTimeMills = 1000;

    // notify consumerId changed 开关
    private boolean notifyConsumerIdsChangedEnable = true;

    // slave 是否需要纠正位点
    private boolean offsetCheckInSlave = false;

    /**
     * 本机名称 Description: <br>
     * 
     * @author wang.jun<br>
     * @taskId <br>
     * @return <br>
     */
    public static String localHostName() {
        try {
            return InetAddress.getLocalHost().getHostName();
        }
        catch (UnknownHostException e) {
            e.printStackTrace();
        }

        return "DEFAULT_BROKER";
    }

    public String getZmqHome() {
        return zmqHome;
    }

    public void setZmqHome(String zmqHome) {
        this.zmqHome = zmqHome;
    }

    public String getNamesrvAddr() {
        return namesrvAddr;
    }

    public void setNamesrvAddr(String namesrvAddr) {
        this.namesrvAddr = namesrvAddr;
    }

    public String getBrokerIP1() {
        return brokerIP1;
    }

    public void setBrokerIP1(String brokerIP1) {
        this.brokerIP1 = brokerIP1;
    }

    public String getBrokerIP2() {
        return brokerIP2;
    }

    public void setBrokerIP2(String brokerIP2) {
        this.brokerIP2 = brokerIP2;
    }

    public String getBrokerName() {
        return brokerName;
    }

    public void setBrokerName(String brokerName) {
        this.brokerName = brokerName;
    }

    public String getBrokerClusterName() {
        return brokerClusterName;
    }

    public void setBrokerClusterName(String brokerClusterName) {
        this.brokerClusterName = brokerClusterName;
    }

    public long getBrokerId() {
        return brokerId;
    }

    public void setBrokerId(long brokerId) {
        this.brokerId = brokerId;
    }

    public int getBrokerPermission() {
        return brokerPermission;
    }

    public void setBrokerPermission(int brokerPermission) {
        this.brokerPermission = brokerPermission;
    }

    public int getDefaultTopicQueueNums() {
        return defaultTopicQueueNums;
    }

    public void setDefaultTopicQueueNums(int defaultTopicQueueNums) {
        this.defaultTopicQueueNums = defaultTopicQueueNums;
    }

    public boolean isAutoCreateTopicEnable() {
        return autoCreateTopicEnable;
    }

    public void setAutoCreateTopicEnable(boolean autoCreateTopicEnable) {
        this.autoCreateTopicEnable = autoCreateTopicEnable;
    }

    public boolean isClusterTopicEnable() {
        return clusterTopicEnable;
    }

    public void setClusterTopicEnable(boolean clusterTopicEnable) {
        this.clusterTopicEnable = clusterTopicEnable;
    }

    public boolean isBrokerTopicEnable() {
        return brokerTopicEnable;
    }

    public void setBrokerTopicEnable(boolean brokerTopicEnable) {
        this.brokerTopicEnable = brokerTopicEnable;
    }

    public boolean isAutoCreateSubscriptionGroup() {
        return autoCreateSubscriptionGroup;
    }

    public void setAutoCreateSubscriptionGroup(boolean autoCreateSubscriptionGroup) {
        this.autoCreateSubscriptionGroup = autoCreateSubscriptionGroup;
    }

    public int getSendMessageThreadPoolNums() {
        return sendMessageThreadPoolNums;
    }

    public void setSendMessageThreadPoolNums(int sendMessageThreadPoolNums) {
        this.sendMessageThreadPoolNums = sendMessageThreadPoolNums;
    }

    public int getPullMessageThreadPoolNums() {
        return pullMessageThreadPoolNums;
    }

    public void setPullMessageThreadPoolNums(int pullMessageThreadPoolNums) {
        this.pullMessageThreadPoolNums = pullMessageThreadPoolNums;
    }

    public int getAdminBroderThreadPoolNums() {
        return adminBroderThreadPoolNums;
    }

    public void setAdminBroderThreadPoolNums(int adminBroderThreadPoolNums) {
        this.adminBroderThreadPoolNums = adminBroderThreadPoolNums;
    }

    public int getClientManageThreadPoolNums() {
        return clientManageThreadPoolNums;
    }

    public void setClientManageThreadPoolNums(int clientManageThreadPoolNums) {
        this.clientManageThreadPoolNums = clientManageThreadPoolNums;
    }

    public int getFlushConsumerOffsetInterval() {
        return flushConsumerOffsetInterval;
    }

    public void setFlushConsumerOffsetInterval(int flushConsumerOffsetInterval) {
        this.flushConsumerOffsetInterval = flushConsumerOffsetInterval;
    }

    public int getFlushConsumerOffsetHistoryInterval() {
        return flushConsumerOffsetHistoryInterval;
    }

    public void setFlushConsumerOffsetHistoryInterval(int flushConsumerOffsetHistoryInterval) {
        this.flushConsumerOffsetHistoryInterval = flushConsumerOffsetHistoryInterval;
    }

    public boolean isRejectTransactionMessage() {
        return rejectTransactionMessage;
    }

    public void setRejectTransactionMessage(boolean rejectTransactionMessage) {
        this.rejectTransactionMessage = rejectTransactionMessage;
    }

    public boolean isFetchNamesrvAddrByAddressServer() {
        return fetchNamesrvAddrByAddressServer;
    }

    public void setFetchNamesrvAddrByAddressServer(boolean fetchNamesrvAddrByAddressServer) {
        this.fetchNamesrvAddrByAddressServer = fetchNamesrvAddrByAddressServer;
    }

    public int getSendThreadPoolQueueCapacity() {
        return sendThreadPoolQueueCapacity;
    }

    public void setSendThreadPoolQueueCapacity(int sendThreadPoolQueueCapacity) {
        this.sendThreadPoolQueueCapacity = sendThreadPoolQueueCapacity;
    }

    public int getPullThreadPoolQueueCapacity() {
        return pullThreadPoolQueueCapacity;
    }

    public void setPullThreadPoolQueueCapacity(int pullThreadPoolQueueCapacity) {
        this.pullThreadPoolQueueCapacity = pullThreadPoolQueueCapacity;
    }

    public int getFilterServerNums() {
        return filterServerNums;
    }

    public void setFilterServerNums(int filterServerNums) {
        this.filterServerNums = filterServerNums;
    }

    public boolean isLongPoolingEnable() {
        return longPoolingEnable;
    }

    public void setLongPoolingEnable(boolean longPoolingEnable) {
        this.longPoolingEnable = longPoolingEnable;
    }

    public long getShortPollingTimeMills() {
        return shortPollingTimeMills;
    }

    public void setShortPollingTimeMills(long shortPollingTimeMills) {
        this.shortPollingTimeMills = shortPollingTimeMills;
    }

    public boolean isNotifyConsumerIdsChangedEnable() {
        return notifyConsumerIdsChangedEnable;
    }

    public void setNotifyConsumerIdsChangedEnable(boolean notifyConsumerIdsChangedEnable) {
        this.notifyConsumerIdsChangedEnable = notifyConsumerIdsChangedEnable;
    }

    public boolean isOffsetCheckInSlave() {
        return offsetCheckInSlave;
    }

    public void setOffsetCheckInSlave(boolean offsetCheckInSlave) {
        this.offsetCheckInSlave = offsetCheckInSlave;
    }

}

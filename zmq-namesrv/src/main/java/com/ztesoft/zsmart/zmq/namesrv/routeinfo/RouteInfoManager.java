package com.ztesoft.zsmart.zmq.namesrv.routeinfo;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.netty.channel.Channel;

import com.ztesoft.zsmart.zmq.common.DataVersion;
import com.ztesoft.zsmart.zmq.common.MixAll;
import com.ztesoft.zsmart.zmq.common.TopicConfig;
import com.ztesoft.zsmart.zmq.common.constant.LoggerName;
import com.ztesoft.zsmart.zmq.common.constant.PermName;
import com.ztesoft.zsmart.zmq.common.namesrv.RegisterBrokerResult;
import com.ztesoft.zsmart.zmq.common.protocol.body.ClusterInfo;
import com.ztesoft.zsmart.zmq.common.protocol.body.TopicConfigSerializeWrapper;
import com.ztesoft.zsmart.zmq.common.protocol.body.TopicList;
import com.ztesoft.zsmart.zmq.common.protocol.route.BrokerData;
import com.ztesoft.zsmart.zmq.common.protocol.route.QueueData;
import com.ztesoft.zsmart.zmq.common.protocol.route.TopicRouteData;
import com.ztesoft.zsmart.zmq.common.sysflag.TopicSysFlag;
import com.ztesoft.zsmart.zmq.remoting.common.RemotingUtil;

/**
 * 运行过程中的路由信息 数据只在内存中 宕机后数据消失 但是Broker会定期推送最新数据 <br>
 * 
 * @author wang.jun<br>
 * @version 1.0<br>
 * @taskId <br>
 * @CreateDate 2016年3月21日 <br>
 * @since V7.3<br>
 * @see com.ztesoft.zsmart.zmq.namesrv.routeinfo <br>
 */
public class RouteInfoManager {
    private static final Logger log = LoggerFactory.getLogger(LoggerName.NamesrvLoggerName);

    private final ReadWriteLock lock = new ReentrantReadWriteLock();

    private final HashMap<String /* topic */, List<QueueData>> topicQueueTable;

    private final HashMap<String /* brokerName */, BrokerData> brokerAddrTable;

    private final HashMap<String /* clusterName */, Set<String/* brokerName */>> clusterAddrTable;

    private final HashMap<String /* broderAddr */, BrokerLiveInfo> brokerLiveTable;

    private final HashMap<String /* broderAddr */, List<String> /* Filter Server */> filterServerTable;

    public RouteInfoManager() {
        this.topicQueueTable = new HashMap<String, List<QueueData>>(1024);
        this.brokerAddrTable = new HashMap<String, BrokerData>(128);
        this.clusterAddrTable = new HashMap<String, Set<String>>(32);
        this.brokerLiveTable = new HashMap<String, BrokerLiveInfo>(256);
        this.filterServerTable = new HashMap<String, List<String>>(256);
    }

    /**
     * 获取所有集群信息: <br>
     * 
     * @author wang.jun<br>
     * @taskId <br>
     * @return <br>
     */
    public byte[] getAllClusterInfo() {
        ClusterInfo clusterInfoSerializeWrapper = new ClusterInfo();
        clusterInfoSerializeWrapper.setBrokerAddrTable(this.brokerAddrTable);
        clusterInfoSerializeWrapper.setClusterAddrTable(clusterAddrTable);
        return clusterInfoSerializeWrapper.encode();
    }

    /**
     * 删除topic: <br>
     * 
     * @author wang.jun<br>
     * @taskId <br>
     * @param topic <br>
     */
    public void deleteTopic(final String topic) {

        try {
            try {
                this.lock.writeLock().lockInterruptibly();
                this.topicQueueTable.remove(topic);
            }
            finally {
                this.lock.writeLock().unlock();
            }
        }
        catch (InterruptedException e) {
            log.error("deleteTopic Exception", e);
        }

    }

    /**
     * 获取所有topicList: <br>
     * 
     * @author wang.jun<br>
     * @taskId <br>
     * @return <br>
     */
    public byte[] getAllTopicList() {
        TopicList topicList = new TopicList();
        try {
            try {
                this.lock.readLock().lockInterruptibly();
                topicList.getTopicList().addAll(this.topicQueueTable.keySet());
            }
            finally {
                this.lock.readLock().unlock();
            }
        }
        catch (InterruptedException e) {
            log.error("getAllTopicList Exception", e);
        }
        return topicList.encode();
    }

    /**
     * 如果是slave 则返回master的ha地址 Description: <br>
     * 
     * @author wang.jun<br>
     * @taskId <br>
     * @return <br>
     */
    public RegisterBrokerResult registerBroker(//
        final String clusterName,// 1
        final String brokerAddr,// 2
        final String brokerName,// 3
        final long brokerId,// 4
        final String haServerAddr,// 5
        final TopicConfigSerializeWrapper topicConfigWrapper,// 6
        final List<String> filterServerList,// 7
        final Channel channel // 8
    ) {

        RegisterBrokerResult result = new RegisterBrokerResult();

        try {
            try {
                this.lock.writeLock().lockInterruptibly();

                // 更新集群信息
                Set<String> brokerNames = this.clusterAddrTable.get(clusterName);
                if (brokerNames == null) {
                    brokerNames = new HashSet<String>();
                    this.clusterAddrTable.put(clusterName, brokerNames);
                }

                brokerNames.add(brokerName);

                boolean registerFirst = false;

                // 更新主备信息
                BrokerData brokerData = this.brokerAddrTable.get(brokerName);
                if (brokerData == null) {
                    registerFirst = true;
                    brokerData = new BrokerData();
                    brokerData.setBrokerName(brokerName);
                    HashMap<Long, String> brokerAddrs = new HashMap<Long, String>();
                    brokerData.setBrokerAddrs(brokerAddrs);
                    this.brokerAddrTable.put(brokerName, brokerData);
                }

                String oldAddr = brokerData.getBrokerAddrs().put(brokerId, brokerAddr);
                registerFirst = registerFirst || (null == oldAddr);

                // 更新topic信息
                if (null != topicConfigWrapper && MixAll.MASTER_ID == brokerId) {

                    if (this.isBrokerTopicConfigChanged(brokerAddr, topicConfigWrapper.getDataVersion())
                        || registerFirst) {
                        ConcurrentHashMap<String, TopicConfig> tcTable = topicConfigWrapper.getTopicConfigTable();

                        if (tcTable != null) {
                            for (String topic : tcTable.keySet()) {
                                TopicConfig topicConfig = tcTable.get(topic);
                                this.createAndUpdateQueueData(brokerName, topicConfig);
                            }
                        }
                    }
                }

                // 更新最后变量时间
                BrokerLiveInfo prevBrokerLiveInfo = this.brokerLiveTable.put(brokerAddr,
                    new BrokerLiveInfo(System.currentTimeMillis(), topicConfigWrapper.getDataVersion(), channel,
                        haServerAddr));

                if (null == prevBrokerLiveInfo) {
                    log.info("new broker registerd, {} HAServer: {}", brokerAddr, haServerAddr);
                }

                // 更新Filter Server列表
                if (filterServerList != null) {
                    if (filterServerList.isEmpty()) {
                        this.filterServerTable.remove(brokerAddr);
                    }
                    else {
                        this.filterServerTable.put(brokerAddr, filterServerList);
                    }
                }

                // 返回值
                if (MixAll.MASTER_ID != brokerId) {
                    String masterAddr = brokerData.getBrokerAddrs().get(MixAll.MASTER_ID);
                    if (masterAddr != null) {
                        BrokerLiveInfo brokerLiveInfo = this.brokerLiveTable.get(masterAddr);
                        if (brokerLiveInfo != null) {
                            result.setHaServerAddr(brokerLiveInfo.getHaServerAddr());
                            result.setMasterAddr(masterAddr);
                        }
                    }
                }
            }
            finally {
                this.lock.writeLock().unlock();
            }
        }
        catch (InterruptedException e) {
            log.error("registerBroker Exception", e);
        }

        return result;
    }

    public int wipeWritePermOfBrokerByLock(final String brokerName) {
        try {
            try {
                this.lock.writeLock().lockInterruptibly();
                return wipeWritePermOfBroker(brokerName);
            }
            finally {
                this.lock.writeLock().unlock();
            }
        }
        catch (Exception e) {
            log.error("wipeWritePermOfBrokerByLock Exception", e);
        }

        return 0;
    }

    private int wipeWritePermOfBroker(final String brokerName) {
        int wipeTopicCnt = 0;
        Iterator<Entry<String, List<QueueData>>> itTopic = this.topicQueueTable.entrySet().iterator();
        while (itTopic.hasNext()) {
            Entry<String, List<QueueData>> entry = itTopic.next();
            List<QueueData> qdList = entry.getValue();

            Iterator<QueueData> it = qdList.iterator();
            while (it.hasNext()) {
                QueueData qd = it.next();
                if (qd.getBrokerName().equals(brokerName)) {
                    int perm = qd.getPerm();
                    perm &= ~PermName.PERM_WRITE;
                    qd.setPerm(perm);
                    wipeTopicCnt++;
                }
            }
        }

        return wipeTopicCnt;
    }

    /**
     * 判断Topic配置信息是否发生变更
     */
    private boolean isBrokerTopicConfigChanged(final String brokerAddr, final DataVersion dataVersion) {
        BrokerLiveInfo prev = this.brokerLiveTable.get(brokerAddr);

        if (null == prev || !prev.getDataVersion().equals(dataVersion)) {
            return true;
        }

        return false;
    }

    /**
     * 创建topicQueue : <br>
     * 
     * @author wang.jun<br>
     * @taskId <br>
     * @param brokerName
     * @param topicConfig <br>
     */
    private void createAndUpdateQueueData(final String brokerName, final TopicConfig topicConfig) {
        QueueData queueData = new QueueData();
        queueData.setBrokerName(brokerName);
        queueData.setWriteQueueNums(topicConfig.getWriteQueueNums());
        queueData.setReadQueueNums(topicConfig.getReadQueueNums());
        queueData.setPerm(topicConfig.getPerm());
        queueData.setTopicSynFlag(topicConfig.getTopicSysFlag());

        List<QueueData> queueDataList = this.topicQueueTable.get(topicConfig.getTopicName());

        if (null == queueDataList) {
            queueDataList = new LinkedList<QueueData>();
            queueDataList.add(queueData);
            this.topicQueueTable.put(topicConfig.getTopicName(), queueDataList);
            log.info("new topic registerd, {} {}", topicConfig.getTopicName(), queueData);
            log.info("new topic registerd, {} {}", topicConfig.getTopicName(), queueData);
        }
        else {
            boolean addNewOne = true;

            Iterator<QueueData> it = queueDataList.iterator();
            while (it.hasNext()) {
                QueueData qd = it.next();
                if (qd.getBrokerName().equals(brokerName)) {
                    if (qd.equals(queueData)) {
                        addNewOne = false;
                    }
                    else {
                        log.info("topic changed, {} OLD: {} NEW: {}", topicConfig.getTopicName(), qd, queueData);
                        it.remove();
                    }
                }
            }

            if (addNewOne) {
                queueDataList.add(queueData);
            }
        }
    }

    public void unregisterBroker(//
        final String clusterName,// 1
        final String brokerAddr,// 2
        final String brokerName,// 3
        final long brokerId// 4
    ) {
        try {
            try {
                this.lock.writeLock().lockInterruptibly();
                BrokerLiveInfo brokerLiveInfo = this.brokerLiveTable.remove(brokerAddr);
                if (brokerLiveInfo != null) {
                    log.info("unregisterBroker, remove from brokerLiveTable {}, {}", //
                        (brokerLiveInfo != null ? "OK" : "Failed"),//
                        brokerAddr//
                    );
                }

                this.filterServerTable.remove(brokerAddr);

                boolean removeBrokerName = false;
                BrokerData brokerData = this.brokerAddrTable.get(brokerName);
                if (null != brokerData) {
                    String addr = brokerData.getBrokerAddrs().remove(brokerId);
                    log.info("unregisterBroker, remove addr from brokerAddrTable {}, {}", //
                        (addr != null ? "OK" : "Failed"),//
                        brokerAddr//
                    );

                    if (brokerData.getBrokerAddrs().isEmpty()) {
                        this.brokerAddrTable.remove(brokerName);
                        log.info("unregisterBroker, remove name from brokerAddrTable OK, {}", //
                            brokerName//
                        );

                        removeBrokerName = true;
                    }
                }

                if (removeBrokerName) {
                    Set<String> nameSet = this.clusterAddrTable.get(clusterName);
                    if (nameSet != null) {
                        boolean removed = nameSet.remove(brokerName);
                        log.info("unregisterBroker, remove name from clusterAddrTable {}, {}", //
                            (removed ? "OK" : "Failed"),//
                            brokerName//
                        );

                        if (nameSet.isEmpty()) {
                            this.clusterAddrTable.remove(clusterName);
                            log.info("unregisterBroker, remove cluster from clusterAddrTable {}", //
                                clusterName//
                            );
                        }
                    }

                    // 删除相应的topic
                    this.removeTopicByBrokerName(brokerName);
                }
            }
            finally {
                this.lock.writeLock().unlock();
            }
        }
        catch (Exception e) {
            log.error("unregisterBroker Exception", e);
        }
    }

    private void removeTopicByBrokerName(final String brokerName) {
        Iterator<Entry<String, List<QueueData>>> itMap = this.topicQueueTable.entrySet().iterator();
        while (itMap.hasNext()) {
            Entry<String, List<QueueData>> entry = itMap.next();

            String topic = entry.getKey();
            List<QueueData> queueDataList = entry.getValue();
            Iterator<QueueData> it = queueDataList.iterator();
            while (it.hasNext()) {
                QueueData qd = it.next();
                if (qd.getBrokerName().equals(brokerName)) {
                    log.info("removeTopicByBrokerName, remove one broker's topic {} {}", topic, qd);
                    it.remove();
                }
            }

            if (queueDataList.isEmpty()) {
                log.info("removeTopicByBrokerName, remove the topic all queue {}", topic);
                itMap.remove();
            }
        }
    }

    public TopicRouteData pickupTopicRouteData(final String topic) {
        TopicRouteData topicRouteData = new TopicRouteData();
        boolean foundQueueData = false;
        boolean foundBrokerData = false;
        Set<String> brokerNameSet = new HashSet<String>();
        List<BrokerData> brokerDataList = new LinkedList<BrokerData>();
        topicRouteData.setBrokerDatas(brokerDataList);

        HashMap<String, List<String>> filterServerMap = new HashMap<String, List<String>>();
        topicRouteData.setFilterServerTable(filterServerMap);

        try {
            try {
                this.lock.readLock().lockInterruptibly();
                List<QueueData> queueDataList = this.topicQueueTable.get(topic);
                if (queueDataList != null) {
                    topicRouteData.setQueueDatas(queueDataList);
                    foundQueueData = true;

                    // BrokerName去重
                    Iterator<QueueData> it = queueDataList.iterator();
                    while (it.hasNext()) {
                        QueueData qd = it.next();
                        brokerNameSet.add(qd.getBrokerName());
                    }

                    for (String brokerName : brokerNameSet) {
                        BrokerData brokerData = this.brokerAddrTable.get(brokerName);
                        if (null != brokerData) {
                            BrokerData brokerDataClone = new BrokerData();
                            brokerDataClone.setBrokerName(brokerData.getBrokerName());
                            brokerDataClone.setBrokerAddrs((HashMap<Long, String>) brokerData.getBrokerAddrs().clone());
                            brokerDataList.add(brokerDataClone);
                            foundBrokerData = true;

                            // 增加Filter Server
                            for (final String brokerAddr : brokerDataClone.getBrokerAddrs().values()) {
                                List<String> filterServerList = this.filterServerTable.get(brokerAddr);
                                filterServerMap.put(brokerAddr, filterServerList);
                            }
                        }
                    }
                }
            }
            finally {
                this.lock.readLock().unlock();
            }
        }
        catch (Exception e) {
            log.error("pickupTopicRouteData Exception", e);
        }

        if (log.isDebugEnabled()) {
            log.debug("pickupTopicRouteData {} {}", topic, topicRouteData);
        }

        if (foundBrokerData && foundQueueData) {
            return topicRouteData;
        }

        return null;
    }

    // Broker Channel两分钟过期
    private final static long BrokerChannelExpiredTime = 1000 * 60 * 2;

    public void scanNotActiveBroker() {
        Iterator<Entry<String, BrokerLiveInfo>> it = this.brokerLiveTable.entrySet().iterator();
        while (it.hasNext()) {
            Entry<String, BrokerLiveInfo> next = it.next();
            long last = next.getValue().getLastUpdateTimestamp();
            if ((last + BrokerChannelExpiredTime) < System.currentTimeMillis()) {
                RemotingUtil.closeChannel(next.getValue().getChannel());
                it.remove();
                log.warn("The broker channel expired, {} {}ms", next.getKey(), BrokerChannelExpiredTime);
                this.onChannelDestroy(next.getKey(), next.getValue().getChannel());
            }
        }
    }

    /**
     * Channel被关闭，或者Channel Idle时间超限
     */
    public void onChannelDestroy(String remoteAddr, Channel channel) {
        String brokerAddrFound = null;

        // 加读锁，寻找断开连接的Broker
        if (channel != null) {
            try {
                try {
                    this.lock.readLock().lockInterruptibly();
                    Iterator<Entry<String, BrokerLiveInfo>> itBrokerLiveTable = this.brokerLiveTable.entrySet()
                        .iterator();
                    while (itBrokerLiveTable.hasNext()) {
                        Entry<String, BrokerLiveInfo> entry = itBrokerLiveTable.next();
                        if (entry.getValue().getChannel() == channel) {
                            brokerAddrFound = entry.getKey();
                            break;
                        }
                    }
                }
                finally {
                    this.lock.readLock().unlock();
                }
            }
            catch (Exception e) {
                log.error("onChannelDestroy Exception", e);
            }
        }

        if (null == brokerAddrFound) {
            brokerAddrFound = remoteAddr;
        }
        else {
            log.info("the broker's channel destroyed, {}, clean it's data structure at once", brokerAddrFound);
        }

        // 加写锁，删除相关数据结构
        if (brokerAddrFound != null && brokerAddrFound.length() > 0) {

            try {
                try {
                    this.lock.writeLock().lockInterruptibly();
                    // 清理brokerLiveTable
                    this.brokerLiveTable.remove(brokerAddrFound);

                    // 清理Filter Server
                    this.filterServerTable.remove(brokerAddrFound);

                    // 清理brokerAddrTable
                    String brokerNameFound = null;
                    boolean removeBrokerName = false;
                    Iterator<Entry<String, BrokerData>> itBrokerAddrTable = this.brokerAddrTable.entrySet().iterator();
                    while (itBrokerAddrTable.hasNext() && (null == brokerNameFound)) {
                        BrokerData brokerData = itBrokerAddrTable.next().getValue();

                        // 遍历Master/Slave，删除brokerAddr
                        Iterator<Entry<Long, String>> it = brokerData.getBrokerAddrs().entrySet().iterator();
                        while (it.hasNext()) {
                            Entry<Long, String> entry = it.next();
                            Long brokerId = entry.getKey();
                            String brokerAddr = entry.getValue();
                            if (brokerAddr.equals(brokerAddrFound)) {
                                brokerNameFound = brokerData.getBrokerName();
                                it.remove();
                                log.info("remove brokerAddr[{}, {}] from brokerAddrTable, because channel destroyed",
                                    brokerId, brokerAddr);
                                break;
                            }
                        }

                        // BrokerName无关联BrokerAddr
                        if (brokerData.getBrokerAddrs().isEmpty()) {
                            removeBrokerName = true;
                            itBrokerAddrTable.remove();
                            log.info("remove brokerName[{}] from brokerAddrTable, because channel destroyed",
                                brokerData.getBrokerName());
                        }
                    }

                    // 清理clusterAddrTable
                    if (brokerNameFound != null && removeBrokerName) {
                        Iterator<Entry<String, Set<String>>> it = this.clusterAddrTable.entrySet().iterator();
                        while (it.hasNext()) {
                            Entry<String, Set<String>> entry = it.next();
                            String clusterName = entry.getKey();
                            Set<String> brokerNames = entry.getValue();
                            boolean removed = brokerNames.remove(brokerNameFound);
                            if (removed) {
                                log.info(
                                    "remove brokerName[{}], clusterName[{}] from clusterAddrTable, because channel destroyed",
                                    brokerNameFound, clusterName);

                                // 如果集群对应的所有broker都下线了， 则集群也删除掉
                                if (brokerNames.isEmpty()) {
                                    log.info(
                                        "remove the clusterName[{}] from clusterAddrTable, because channel destroyed and no broker in this cluster",
                                        clusterName);
                                    it.remove();
                                }

                                break;
                            }
                        }
                    }

                    // 清理topicQueueTable
                    if (removeBrokerName) {
                        Iterator<Entry<String, List<QueueData>>> itTopicQueueTable = this.topicQueueTable.entrySet()
                            .iterator();
                        while (itTopicQueueTable.hasNext()) {
                            Entry<String, List<QueueData>> entry = itTopicQueueTable.next();
                            String topic = entry.getKey();
                            List<QueueData> queueDataList = entry.getValue();

                            Iterator<QueueData> itQueueData = queueDataList.iterator();
                            while (itQueueData.hasNext()) {
                                QueueData queueData = itQueueData.next();
                                if (queueData.getBrokerName().equals(brokerNameFound)) {
                                    itQueueData.remove();
                                    log.info("remove topic[{} {}], from topicQueueTable, because channel destroyed",
                                        topic, queueData);
                                }
                            }

                            if (queueDataList.isEmpty()) {
                                itTopicQueueTable.remove();
                                log.info("remove topic[{}] all queue, from topicQueueTable, because channel destroyed",
                                    topic);
                            }
                        }
                    }
                }
                finally {
                    this.lock.writeLock().unlock();
                }
            }
            catch (Exception e) {
                log.error("onChannelDestroy Exception", e);
            }
        }
    }

    /**
     * 定期打印当前类的数据结构
     */
    public void printAllPeriodically() {
        try {
            try {
                this.lock.readLock().lockInterruptibly();
                log.info("--------------------------------------------------------");
                {
                    log.info("topicQueueTable SIZE: {}", this.topicQueueTable.size());
                    Iterator<Entry<String, List<QueueData>>> it = this.topicQueueTable.entrySet().iterator();
                    while (it.hasNext()) {
                        Entry<String, List<QueueData>> next = it.next();
                        log.info("topicQueueTable Topic: {} {}", next.getKey(), next.getValue());
                    }
                }

                {
                    log.info("brokerAddrTable SIZE: {}", this.brokerAddrTable.size());
                    Iterator<Entry<String, BrokerData>> it = this.brokerAddrTable.entrySet().iterator();
                    while (it.hasNext()) {
                        Entry<String, BrokerData> next = it.next();
                        log.info("brokerAddrTable brokerName: {} {}", next.getKey(), next.getValue());
                    }
                }

                {
                    log.info("brokerLiveTable SIZE: {}", this.brokerLiveTable.size());
                    Iterator<Entry<String, BrokerLiveInfo>> it = this.brokerLiveTable.entrySet().iterator();
                    while (it.hasNext()) {
                        Entry<String, BrokerLiveInfo> next = it.next();
                        log.info("brokerLiveTable brokerAddr: {} {}", next.getKey(), next.getValue());
                    }
                }

                {
                    log.info("clusterAddrTable SIZE: {}", this.clusterAddrTable.size());
                    Iterator<Entry<String, Set<String>>> it = this.clusterAddrTable.entrySet().iterator();
                    while (it.hasNext()) {
                        Entry<String, Set<String>> next = it.next();
                        log.info("clusterAddrTable clusterName: {} {}", next.getKey(), next.getValue());
                    }
                }
            }
            finally {
                this.lock.readLock().unlock();
            }
        }
        catch (Exception e) {
            log.error("printAllPeriodically Exception", e);
        }
    }

    /**
     * 获取指定集群下的所有 topic 列表
     * 
     * @return
     */
    public byte[] getSystemTopicList() {
        TopicList topicList = new TopicList();
        try {
            try {
                this.lock.readLock().lockInterruptibly();
                for (String cluster : clusterAddrTable.keySet()) {
                    topicList.getTopicList().add(cluster);
                    topicList.getTopicList().addAll(this.clusterAddrTable.get(cluster));
                }

                // 随机取一台 broker
                if (brokerAddrTable != null && !brokerAddrTable.isEmpty()) {
                    Iterator<String> it = brokerAddrTable.keySet().iterator();
                    while (it.hasNext()) {
                        BrokerData bd = brokerAddrTable.get(it.next());
                        HashMap<Long, String> brokerAddrs = bd.getBrokerAddrs();
                        if (bd.getBrokerAddrs() != null && !bd.getBrokerAddrs().isEmpty()) {
                            Iterator<Long> it2 = brokerAddrs.keySet().iterator();
                            topicList.setBrokerAddr(brokerAddrs.get(it2.next()));
                            break;
                        }
                    }
                }
            }
            finally {
                this.lock.readLock().unlock();
            }
        }
        catch (Exception e) {
            log.error("getAllTopicList Exception", e);
        }

        return topicList.encode();
    }
    
    /**
     * 获取指定集群下的所有 topic 列表
     * 
     * @param cluster
     * @return
     */
    public byte[] getTopicsByCluster(String cluster) {
        TopicList topicList = new TopicList();
        try {
            try {
                this.lock.readLock().lockInterruptibly();
                Set<String> brokerNameSet = this.clusterAddrTable.get(cluster);
                for (String brokerName : brokerNameSet) {
                    Iterator<Entry<String, List<QueueData>>> topicTableIt =
                            this.topicQueueTable.entrySet().iterator();
                    while (topicTableIt.hasNext()) {
                        Entry<String, List<QueueData>> topicEntry = topicTableIt.next();
                        String topic = topicEntry.getKey();
                        List<QueueData> queueDatas = topicEntry.getValue();
                        for (QueueData queueData : queueDatas) {
                            if (brokerName.equals(queueData.getBrokerName())) {
                                topicList.getTopicList().add(topic);
                                break;
                            }
                        }
                    }
                }
            }
            finally {
                this.lock.readLock().unlock();
            }
        }
        catch (Exception e) {
            log.error("getAllTopicList Exception", e);
        }

        return topicList.encode();
    }


    /**
     * 获取单元逻辑下的所有 topic 列表
     * 
     * @return
     */
    public byte[] getUnitTopics() {
        TopicList topicList = new TopicList();
        try {
            try {
                this.lock.readLock().lockInterruptibly();
                Iterator<Entry<String, List<QueueData>>> topicTableIt =
                        this.topicQueueTable.entrySet().iterator();
                while (topicTableIt.hasNext()) {
                    Entry<String, List<QueueData>> topicEntry = topicTableIt.next();
                    String topic = topicEntry.getKey();
                    List<QueueData> queueDatas = topicEntry.getValue();
                    if (queueDatas != null && queueDatas.size() > 0
                            && TopicSysFlag.hasUnitFlag(queueDatas.get(0).getTopicSynFlag())) {
                        topicList.getTopicList().add(topic);
                    }
                }
            }
            finally {
                this.lock.readLock().unlock();
            }
        }
        catch (Exception e) {
            log.error("getAllTopicList Exception", e);
        }

        return topicList.encode();
    }


    /**
     * 获取中心向单元同步的所有 topic 列表
     * 
     * @return
     */
    public byte[] getHasUnitSubTopicList() {
        TopicList topicList = new TopicList();
        try {
            try {
                this.lock.readLock().lockInterruptibly();
                Iterator<Entry<String, List<QueueData>>> topicTableIt =
                        this.topicQueueTable.entrySet().iterator();
                while (topicTableIt.hasNext()) {
                    Entry<String, List<QueueData>> topicEntry = topicTableIt.next();
                    String topic = topicEntry.getKey();
                    List<QueueData> queueDatas = topicEntry.getValue();
                    if (queueDatas != null && queueDatas.size() > 0
                            && TopicSysFlag.hasUnitSubFlag(queueDatas.get(0).getTopicSynFlag())) {
                        topicList.getTopicList().add(topic);
                    }
                }
            }
            finally {
                this.lock.readLock().unlock();
            }
        }
        catch (Exception e) {
            log.error("getAllTopicList Exception", e);
        }

        return topicList.encode();
    }


    /**
     * 获取含有单元化订阅组的非单元化 Topic 列表
     * 
     * @return
     */
    public byte[] getHasUnitSubUnUnitTopicList() {
        TopicList topicList = new TopicList();
        try {
            try {
                this.lock.readLock().lockInterruptibly();
                Iterator<Entry<String, List<QueueData>>> topicTableIt =
                        this.topicQueueTable.entrySet().iterator();
                while (topicTableIt.hasNext()) {
                    Entry<String, List<QueueData>> topicEntry = topicTableIt.next();
                    String topic = topicEntry.getKey();
                    List<QueueData> queueDatas = topicEntry.getValue();
                    if (queueDatas != null && queueDatas.size() > 0
                            && !TopicSysFlag.hasUnitFlag(queueDatas.get(0).getTopicSynFlag())
                            && TopicSysFlag.hasUnitSubFlag(queueDatas.get(0).getTopicSynFlag())) {
                        topicList.getTopicList().add(topic);
                    }
                }
            }
            finally {
                this.lock.readLock().unlock();
            }
        }
        catch (Exception e) {
            log.error("getAllTopicList Exception", e);
        }

        return topicList.encode();
    }

    class BrokerLiveInfo {
        private long lastUpdateTimestamp;

        private DataVersion dataVersion;

        private Channel channel;

        private String haServerAddr;

        public BrokerLiveInfo(long lastUpdateTimestamp, DataVersion dataVersion, Channel channel, String haServerAddr) {
            super();
            this.lastUpdateTimestamp = lastUpdateTimestamp;
            this.dataVersion = dataVersion;
            this.channel = channel;
            this.haServerAddr = haServerAddr;
        }

        public long getLastUpdateTimestamp() {
            return lastUpdateTimestamp;
        }

        public void setLastUpdateTimestamp(long lastUpdateTimestamp) {
            this.lastUpdateTimestamp = lastUpdateTimestamp;
        }

        public DataVersion getDataVersion() {
            return dataVersion;
        }

        public void setDataVersion(DataVersion dataVersion) {
            this.dataVersion = dataVersion;
        }

        public Channel getChannel() {
            return channel;
        }

        public void setChannel(Channel channel) {
            this.channel = channel;
        }

        public String getHaServerAddr() {
            return haServerAddr;
        }

        public void setHaServerAddr(String haServerAddr) {
            this.haServerAddr = haServerAddr;
        }

    }

}

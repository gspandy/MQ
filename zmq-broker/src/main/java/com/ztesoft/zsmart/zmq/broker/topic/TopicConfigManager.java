package com.ztesoft.zsmart.zmq.broker.topic;

import java.util.HashSet;
import java.util.Iterator;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.ztesoft.zsmart.zmq.broker.BrokerController;
import com.ztesoft.zsmart.zmq.broker.BrokerPathConfigHelper;
import com.ztesoft.zsmart.zmq.common.ConfigManager;
import com.ztesoft.zsmart.zmq.common.DataVersion;
import com.ztesoft.zsmart.zmq.common.MixAll;
import com.ztesoft.zsmart.zmq.common.TopicConfig;
import com.ztesoft.zsmart.zmq.common.constant.LoggerName;
import com.ztesoft.zsmart.zmq.common.constant.PermName;
import com.ztesoft.zsmart.zmq.common.protocol.body.KVTable;
import com.ztesoft.zsmart.zmq.common.protocol.body.TopicConfigSerializeWrapper;
import com.ztesoft.zsmart.zmq.common.sysflag.TopicSysFlag;
import com.ztesoft.zsmart.zmq.remoting.protocol.RemotingSerializable;

public class TopicConfigManager extends ConfigManager {
    private static final Logger log = LoggerFactory.getLogger(LoggerName.BrokerLoggerName);

    private static final long LockTimeoutMillis = 3000;

    private transient final Lock lockTopicConfigTable = new ReentrantLock();

    private transient BrokerController brokerController;

    // Topic配置
    private final ConcurrentHashMap<String, TopicConfig> topicConfigTable = new ConcurrentHashMap<String, TopicConfig>(
        1024);

    private final DataVersion dataVersion = new DataVersion();

    private final Set<String> systemTopicList = new HashSet<String>();

    public TopicConfigManager() {
    }

    public TopicConfigManager(BrokerController brokerController) {
        this.brokerController = brokerController;

        // MixAll.SELF_TEST_TOPIC
        String topic = MixAll.SELF_TEST_TOPIC;
        TopicConfig topicConfig = new TopicConfig(topic);
        this.systemTopicList.add(topic);
        topicConfig.setReadQueueNums(1);
        topicConfig.setWriteQueueNums(1);
        this.topicConfigTable.put(topic, topicConfig);

        // MixAll.DEFAULT_TOPIC TBW102
        topic = MixAll.DEFAULT_TOPIC;
        topicConfig = new TopicConfig(topic);
        this.systemTopicList.add(topic);
        topicConfig.setReadQueueNums(this.brokerController.getBrokerConfig().getDefaultTopicQueueNums());
        topicConfig.setWriteQueueNums(this.brokerController.getBrokerConfig().getDefaultTopicQueueNums());
        int perm = PermName.PERM_INHERIT | PermName.PERM_READ | PermName.PERM_WRITE;
        topicConfig.setPerm(perm);
        this.topicConfigTable.put(topic, topicConfig);

        // MixAll.BENCHMARK_TOPIC; BenchmarkTest
        topic = MixAll.BENCHMARK_TOPIC;
        topicConfig = new TopicConfig(topic);
        this.systemTopicList.add(topic);
        topicConfig.setReadQueueNums(1024);
        topicConfig.setWriteQueueNums(1024);
        this.topicConfigTable.put(topicConfig.getTopicName(), topicConfig);

        // 集群名称
        topic = this.brokerController.getBrokerConfig().getBrokerClusterName();
        topicConfig = new TopicConfig(topic);
        this.systemTopicList.add(topic);
        perm = PermName.PERM_INHERIT;
        if (this.brokerController.getBrokerConfig().isClusterTopicEnable()) {
            perm |= PermName.PERM_READ | PermName.PERM_WRITE;
        }
        this.topicConfigTable.put(topicConfig.getTopicName(), topicConfig);

        // 服务器名称
        topic = this.brokerController.getBrokerConfig().getBrokerName();
        topicConfig = new TopicConfig(topic);
        this.systemTopicList.add(topic);
        perm = PermName.PERM_INHERIT;
        if (this.brokerController.getBrokerConfig().isBrokerTopicEnable()) {
            perm |= PermName.PERM_READ | PermName.PERM_WRITE;
        }
        topicConfig.setReadQueueNums(1);
        topicConfig.setWriteQueueNums(1);
        topicConfig.setPerm(perm);
        this.topicConfigTable.put(topicConfig.getTopicName(), topicConfig);

        // MixAll.OFFSET_MOVED_EVENT OFFSET_MOVED_EVENT
        // MixAll.OFFSET_MOVED_EVENT
        topic = MixAll.OFFSET_MOVED_EVENT;
        topicConfig = new TopicConfig(topic);
        this.systemTopicList.add(topic);
        topicConfig.setReadQueueNums(1);
        topicConfig.setWriteQueueNums(1);
        this.topicConfigTable.put(topicConfig.getTopicName(), topicConfig);

    }

    public boolean isSystemTopic(final String topic) {
        return this.systemTopicList.contains(topic);
    }

    public Set<String> getSystemTopic() {
        return this.systemTopicList;
    }

    public boolean isTopicCanSendMessage(final String topic) {
        boolean reservedWords = topic.equals(MixAll.DEFAULT_TOPIC)
            || topic.equals(this.brokerController.getBrokerConfig().getBrokerClusterName());

        return !reservedWords;
    }

    public TopicConfig selectTopicConfig(final String topic) {
        return this.topicConfigTable.get(topic);
    }

    /**
     * 发消息时如果topic 不存在 尝试创建topic: <br>
     * 
     * @author wang.jun<br>
     * @taskId <br>
     * @param topic
     * @param defaultTopic
     * @param remoteAddress
     * @param clientDefaultTopicQueueNum
     * @param topicSysFlag
     * @return <br>
     */
    public TopicConfig createTopicInSendMessageMethod(final String topic, final String defaultTopic,
        final String remoteAddress, final int clientDefaultTopicQueueNum, final int topicSysFlag) {
        TopicConfig topicConfig = null;
        boolean createNew = false;

        try {
            try {
                if (this.lockTopicConfigTable.tryLock(LockTimeoutMillis, TimeUnit.MICROSECONDS)) {
                    topicConfig = this.topicConfigTable.get(topic);
                    if (topicConfig != null) {
                        return topicConfig;
                    }

                    TopicConfig defaultTopicConfig = this.topicConfigTable.get(defaultTopic);

                    if (defaultTopicConfig != null) {
                        if (PermName.isInherited(defaultTopicConfig.getPerm())) {
                            topicConfig = new TopicConfig(topic);

                            int queueNums = clientDefaultTopicQueueNum > defaultTopicConfig.getWriteQueueNums() ? defaultTopicConfig
                                .getWriteQueueNums() : clientDefaultTopicQueueNum;

                            if (queueNums < 0) {
                                queueNums = 0;
                            }

                            topicConfig.setReadQueueNums(queueNums);
                            topicConfig.setWriteQueueNums(queueNums);
                            int perm = defaultTopicConfig.getPerm();

                            perm &= ~PermName.PERM_INHERIT;
                            topicConfig.setPerm(perm);
                            topicConfig.setTopicSysFlag(topicSysFlag);
                            topicConfig.setTopicFilterType(defaultTopicConfig.getTopicFilterType());
                        }
                        else {
                            log.warn("create new topic failed, because the default topic[" + defaultTopic
                                + "] no perm, " + defaultTopicConfig.getPerm() + " producer: " + remoteAddress);
                        }

                    }
                    else {
                        log.warn("create new topic failed, because the default topic[" + defaultTopic + "] not exist."
                            + " producer: " + remoteAddress);
                    }

                    if (topicConfig != null) {
                        log.info("create new topic by default topic[" + defaultTopic + "], " + topicConfig
                            + " producer: " + remoteAddress);
                        this.topicConfigTable.put(topic, topicConfig);
                        this.dataVersion.nextVersion();
                        createNew = true;
                        this.persist();
                    }
                }
            }
            finally {
                this.lockTopicConfigTable.unlock();
            }

        }
        catch (Exception e) {
            log.error("createTopicInSendMessageMethod exception", e);
        }

        if (createNew) {
            this.brokerController.registerBrokerAll(false, true);
        }

        return topicConfig;
    }

    public TopicConfig createTopicInSendMessageBackMethod(//
        final String topic,//
        final int clientDefaultTopicNums,//
        final int perm,//
        final int topicSysFlag) {
        TopicConfig topicConfig = this.topicConfigTable.get(topic);

        if (topicConfig != null) {
            return topicConfig;
        }

        boolean createNew = false;

        try {
            try {
                if (this.lockTopicConfigTable.tryLock(LockTimeoutMillis, TimeUnit.MILLISECONDS)) {
                    topicConfig = this.topicConfigTable.get(topic);
                    if (topicConfig != null)
                        return topicConfig;

                    topicConfig = new TopicConfig(topic);
                    topicConfig.setWriteQueueNums(clientDefaultTopicNums);
                    topicConfig.setReadQueueNums(clientDefaultTopicNums);
                    topicConfig.setPerm(perm);
                    topicConfig.setTopicSysFlag(topicSysFlag);
                    log.info("create new topic {}", topicConfig);
                    this.topicConfigTable.put(topic, topicConfig);
                    createNew = true;
                    this.dataVersion.nextVersion();
                    this.persist();
                }
            }
            finally {
                this.lockTopicConfigTable.unlock();
            }
        }
        catch (Exception e) {
            log.error("createTopicInSendMessageBackMethod exception", e);
        }

        if (createNew) {
            this.brokerController.registerBrokerAll(false, true);
        }

        return topicConfig;
    }

    /**
     * 更新topic 的单元化标识: <br>
     * 
     * @author wang.jun<br>
     * @taskId <br>
     * @param topic
     * @param unit <br>
     */
    public void updateTopicUnitFlag(final String topic, final boolean unit) {
        TopicConfig topicConfig = this.topicConfigTable.get(topic);

        if (topicConfig != null) {
            int oldTopicSysFlag = topicConfig.getTopicSysFlag();
            if (unit) {
                topicConfig.setTopicSysFlag(TopicSysFlag.setUnitFlag(oldTopicSysFlag));
            }
            else {
                topicConfig.setTopicSysFlag(TopicSysFlag.clearUnitFlag(oldTopicSysFlag));
            }

            log.info("update topic sys flag. oldTopicSysFlag={}, newTopicSysFlag", oldTopicSysFlag,
                topicConfig.getTopicSysFlag());
            this.topicConfigTable.put(topic, topicConfig);

            this.dataVersion.nextVersion();

            this.persist();
            this.brokerController.registerBrokerAll(false, true);
        }
    }

    /**
     * 更新topic是否有单元化定阅组: <br>
     * 
     * @author wang.jun<br>
     * @taskId <br>
     * @param topic
     * @param unit <br>
     */
    public void updateTopicUnitSubFlag(final String topic, final boolean hasUnitSub) {
        TopicConfig topicConfig = this.topicConfigTable.get(topic);
        if (topicConfig != null) {
            int oldTopicSysFlag = topicConfig.getTopicSysFlag();
            if (hasUnitSub) {
                topicConfig.setTopicSysFlag(TopicSysFlag.setUnitSubFlag(oldTopicSysFlag));
            }

            log.info("update topic sys flag. oldTopicSysFlag={}, newTopicSysFlag", oldTopicSysFlag,
                topicConfig.getTopicSysFlag());

            this.topicConfigTable.put(topic, topicConfig);

            this.dataVersion.nextVersion();

            this.persist();
            this.brokerController.registerBrokerAll(false, true);
        }
    }

    /**
     * 更新topic配置: <br>
     * 
     * @author wang.jun<br>
     * @taskId <br>
     * @param topicConfig <br>
     */
    public void updateTopicConfig(final TopicConfig topicConfig) {
        TopicConfig old = this.topicConfigTable.put(topicConfig.getTopicName(), topicConfig);
        if (old != null) {
            log.info("update topic config, old: " + old + " new: " + topicConfig);
        }
        else {
            log.info("create new topic, " + topicConfig);
        }

        this.dataVersion.nextVersion();

        this.persist();
    }

    /**
     * 更新顺序消息配置: <br>
     * 
     * @author wang.jun<br>
     * @taskId <br>
     * @param orderKVTableFromNs <br>
     */
    public void updateOrderTopicConfig(final KVTable orderKVTableFromNs) {
        // 根据nameserver 上的topic 配置检查更新topic config 的顺序信息配置

        if (orderKVTableFromNs != null && orderKVTableFromNs.getTable() != null) {
            boolean isChange = false;
            Set<String> orderTopics = orderKVTableFromNs.getTable().keySet();
            for (String topic : orderTopics) {
                TopicConfig topicConfig = this.topicConfigTable.get(topic);
                if (topicConfig != null && !topicConfig.isOrder()) {
                    topicConfig.setOrder(true);
                    isChange = true;
                    log.info("update order topic config, topic={}, order={}", topic, true);
                }
            }

            for (String topic : this.topicConfigTable.keySet()) {
                if (!orderTopics.contains(topic)) {
                    TopicConfig topicConfig = this.topicConfigTable.get(topic);
                    if (topicConfig.isOrder()) {
                        topicConfig.setOrder(false);
                        isChange = true;
                        log.info("update order topic config, topic={}, order={}", topic, false);
                    }
                }
            }
            if (isChange) {
                this.dataVersion.nextVersion();
                this.persist();
            }
        }
    }

    /**
     * 判断消息是否为顺序消息: <br>
     * 
     * @author wang.jun<br>
     * @taskId <br>
     * @param topic
     * @return <br>
     */
    public boolean isOrderTopic(final String topic) {
        TopicConfig topicConfig = this.topicConfigTable.get(topic);

        if (topicConfig == null) {
            return false;
        }

        return topicConfig.isOrder();
    }

    /**
     * 删除topic配置: <br>
     * 
     * @author wang.jun<br>
     * @taskId <br>
     * @param topic <br>
     */
    public void deleteTopicConfig(final String topic) {
        TopicConfig old = this.topicConfigTable.remove(topic);
        if (old != null) {
            log.info("delete topic config OK, topic: " + old);
            this.dataVersion.nextVersion();
            this.persist();
        }
        else {
            log.warn("delete topic config failed, topic: " + topic + " not exist");
        }
    }

    public TopicConfigSerializeWrapper buildTopicConfigSerializeWrapper() {
        TopicConfigSerializeWrapper wrapper = new TopicConfigSerializeWrapper();
        wrapper.setDataVersion(dataVersion);
        wrapper.setTopicConfigTable(topicConfigTable);
        return wrapper;
    }

    @Override
    public String encode() {
        return encode(false);
    }

    @Override
    public String encode(boolean prettyFormat) {
        return buildTopicConfigSerializeWrapper().toJson();
    }

    @Override
    public void decode(String jsonString) {
        if (jsonString != null) {
            TopicConfigSerializeWrapper wrapper = RemotingSerializable.fromJson(jsonString,
                TopicConfigSerializeWrapper.class);

            if (wrapper != null) {
                this.topicConfigTable.putAll(wrapper.getTopicConfigTable());
                this.dataVersion.assignNewOne(wrapper.getDataVersion());
                this.printLoadDataWhenFirstBoot(wrapper);
            }
        }

    }

    private void printLoadDataWhenFirstBoot(TopicConfigSerializeWrapper tcs) {
        Iterator<Entry<String, TopicConfig>> it = tcs.getTopicConfigTable().entrySet().iterator();
        while (it.hasNext()) {
            Entry<String, TopicConfig> next = it.next();
            log.info("load exist local topic, {}", next.getValue().toString());
        }

    }

    @Override
    public String configFilePath() {
        return BrokerPathConfigHelper.getTopicConfigPath(this.brokerController.getMessageStoreConfig()
            .getStorePathRootDir());
    }

    public DataVersion getDataVersion() {
        return dataVersion;
    }

    public ConcurrentHashMap<String, TopicConfig> getTopicConfigTable() {
        return topicConfigTable;
    }

}

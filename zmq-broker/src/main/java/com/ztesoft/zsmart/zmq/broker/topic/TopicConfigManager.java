package com.ztesoft.zsmart.zmq.broker.topic;

import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.ztesoft.zsmart.zmq.broker.BrokerController;
import com.ztesoft.zsmart.zmq.common.ConfigManager;
import com.ztesoft.zsmart.zmq.common.DataVersion;
import com.ztesoft.zsmart.zmq.common.MixAll;
import com.ztesoft.zsmart.zmq.common.TopicConfig;
import com.ztesoft.zsmart.zmq.common.constant.LoggerName;
import com.ztesoft.zsmart.zmq.common.constant.PermName;

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
    
    public TopicConfig selectTopicConfig(final String topic){
        return this.topicConfigTable.get(topic);
    }

    @Override
    public String encode() {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public String encode(boolean prettyFormat) {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public void decode(String jsonString) {
        // TODO Auto-generated method stub

    }

    @Override
    public String configFilePath() {
        // TODO Auto-generated method stub
        return null;
    }

}

package com.ztesoft.zsmart.zmq.broker.offset;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.ztesoft.zsmart.zmq.broker.BrokerController;
import com.ztesoft.zsmart.zmq.broker.BrokerPathConfigHelper;
import com.ztesoft.zsmart.zmq.common.ConfigManager;
import com.ztesoft.zsmart.zmq.common.UtilAll;
import com.ztesoft.zsmart.zmq.common.constant.LoggerName;
import com.ztesoft.zsmart.zmq.remoting.protocol.RemotingSerializable;


/**
 * 消费进度管理 <br>
 * 
 * @author wang.jun<br>
 * @version 1.0<br>
 * @taskId <br>
 * @CreateDate 2016年4月1日 <br>
 * @since V7.3<br>
 * @see com.ztesoft.zsmart.zmq.broker.offset <br>
 */
public class ConsumerOffsetManager extends ConfigManager {
    private static final Logger log = LoggerFactory.getLogger(LoggerName.BrokerLoggerName);

    private static final String TOPIC_GROUP_SEPARATOR = "@";

    private ConcurrentHashMap<String /* topic@group */, ConcurrentHashMap<Integer, Long>> offsetTable =
            new ConcurrentHashMap<String, ConcurrentHashMap<Integer, Long>>();

    private transient BrokerController brokerController;


    public ConsumerOffsetManager() {
    }


    public ConsumerOffsetManager(BrokerController brokerController) {
        this.brokerController = brokerController;
    }


    /**
     * 
     * 扫描数据被删除的topic,offset记录也对应删除: <br>
     * 
     * @author wang.jun<br>
     * @taskId <br>
     *         <br>
     */
    public void scanUnsubscribedTopic() {
        Iterator<Entry<String, ConcurrentHashMap<Integer, Long>>> it = this.offsetTable.entrySet().iterator();
        while (it.hasNext()) {
            Entry<String, ConcurrentHashMap<Integer, Long>> next = it.next();
            String topicAtGroup = next.getKey();
            String[] arrays = topicAtGroup.split(TOPIC_GROUP_SEPARATOR);
            if (arrays != null && arrays.length == 2) {
                String topic = arrays[0];
                String group = arrays[1];

                // 当前订阅关系没有group-topic订阅关系 （消费端当前是停机的状态 ）并且offset落后很多 则删除消费进度
                if (null == brokerController.getConsumerManager().findSubscriptionData(group, topic)
                        && this.offsetBehindMuchThanData(topic, next.getValue())) {
                    it.remove();
                    log.warn("remove topic offset, {}", topicAtGroup);
                }
            }
        }
    }


    /**
     * 
     * 判断消费端是否落后: <br>
     * 
     * @author wang.jun<br>
     * @taskId <br>
     * @param topic
     * @param value
     * @return <br>
     */
    private boolean offsetBehindMuchThanData(String topic, ConcurrentHashMap<Integer, Long> table) {
        Iterator<Entry<Integer, Long>> it = table.entrySet().iterator();

        boolean result = !table.isEmpty();
        while (it.hasNext() && result) {
            Entry<Integer, Long> next = it.next();
            long minOffsetInStore =
                    this.brokerController.getMessageStore().getMinOffsetInQuque(topic, next.getKey());
            long offsetInPersist = next.getValue();
            if (offsetInPersist > minOffsetInStore) {
                return false;
            }
        }
        return result;
    }


    /**
     * 根据group 查询topic
     * 
     * @param group
     * @return
     */
    public Set<String> whichTopicByConsumer(final String group) {
        Set<String> topics = new HashSet<String>();

        for (String keys : this.offsetTable.keySet()) {
            String[] arrays = keys.split(TOPIC_GROUP_SEPARATOR);
            if (arrays != null && arrays.length == 2 && arrays[1].equals(group)) {
                topics.add(arrays[0]);
            }
        }

        return topics;
    }


    /**
     * 根据topic加载group
     * 
     * @param topic
     * @return
     */
    public Set<String> whichGroupByTopic(final String topic) {
        Set<String> groups = new HashSet<String>();

        for (String keys : this.offsetTable.keySet()) {
            String[] arrays = keys.split(TOPIC_GROUP_SEPARATOR);
            if (arrays != null && arrays.length == 2 && arrays[0].equals(topic)) {
                groups.add(arrays[1]);
            }
        }

        return groups;
    }


    /**
     * 提交消费进度
     * 
     * @param group
     * @param topic
     * @param queueId
     * @param offset
     */
    public void commitOffset(final String group, final String topic, final int queueId, final long offset) {
        // key 为 topic@group
        String key = topic + TOPIC_GROUP_SEPARATOR + group;
        this.commitOffset(key, queueId, offset);
    }


    private void commitOffset(String key, int queueId, long offset) {
        ConcurrentHashMap<Integer, Long> map = this.offsetTable.get(key);
        if (null == map) {
            map = new ConcurrentHashMap<Integer, Long>();
            map.put(queueId, offset);
            this.offsetTable.put(key, map);
        }
        else {
            map.put(queueId, offset);
        }
    }


    /**
     * 查询消费进度
     * 
     * @param group
     * @param topic
     * @param queueId
     * @return
     */
    public long queryOffset(final String group, final String topic, final int queueId) {
        // key = topic@group
        String key = topic + TOPIC_GROUP_SEPARATOR + group;
        ConcurrentHashMap<Integer, Long> map = this.offsetTable.get(key);

        if (null != null) {
            Long offset = map.get(queueId);
            if (offset != null) {
                return offset;
            }
        }

        return -1;
    }


    @Override
    public String encode() {
        return this.encode(false);
    }


    @Override
    public String encode(boolean prettyFormat) {
        return RemotingSerializable.toJson(this, prettyFormat);
    }


    @Override
    public void decode(String jsonString) {
        if (jsonString != null) {
            ConsumerOffsetManager obj =
                    RemotingSerializable.fromJson(jsonString, ConsumerOffsetManager.class);
            if (obj != null) {
                this.offsetTable = obj.getOffsetTable();
            }
        }

    }


    @Override
    public String configFilePath() {
        return BrokerPathConfigHelper
            .getConsumerOffsetPath(this.brokerController.getMessageStoreConfig().getStorePathRootDir());
    }


    public ConcurrentHashMap<String, ConcurrentHashMap<Integer, Long>> getOffsetTable() {
        return offsetTable;
    }


    public void setOffsetTable(ConcurrentHashMap<String, ConcurrentHashMap<Integer, Long>> offsetTable) {
        this.offsetTable = offsetTable;
    }


    public Map<Integer, Long> queryMinOffsetInAllGroup(final String topic, final String filterGroups) {
        Map<Integer, Long> queueMinOffset = new HashMap<Integer, Long>();
        Set<String> topicGroups = this.offsetTable.keySet();

        if (!UtilAll.isBlank(filterGroups)) {
            for (String group : filterGroups.split(",")) {
                Iterator<String> it = topicGroups.iterator();
                while (it.hasNext()) {
                    if (group.equals(it.next().split(TOPIC_GROUP_SEPARATOR)[1])) {
                        it.remove();
                    }
                }
            }
        }

        for (String topicGroup : topicGroups) {
            String[] topicGroupArr = topicGroup.split(TOPIC_GROUP_SEPARATOR);
            if (topic.equals(topicGroupArr[0])) {
                for (Entry<Integer, Long> entry : this.offsetTable.get(topicGroup).entrySet()) {
                    long minOffset = this.brokerController.getMessageStore().getMinOffsetInQuque(topic,
                        entry.getKey());
                    if (entry.getValue() >= minOffset) {
                        Long offset = queueMinOffset.get(entry.getKey());
                        if (offset == null) {
                            queueMinOffset.put(entry.getKey(), Math.min(Long.MAX_VALUE, entry.getValue()));
                        }
                        else {
                            queueMinOffset.put(entry.getKey(), Math.min(entry.getValue(), offset));
                        }
                    }
                }
            }
        }
        return queueMinOffset;
    }


    public Map<Integer, Long> queryOffset(final String group, final String topic) {
        // topic@group
        String key = topic + TOPIC_GROUP_SEPARATOR + group;
        return this.offsetTable.get(key);
    }


    public void cloneOffset(final String srcGroup, final String destGroup, final String topic) {
        ConcurrentHashMap<Integer, Long> offsets =
                this.offsetTable.get(topic + TOPIC_GROUP_SEPARATOR + srcGroup);
        if (offsets != null) {
            this.offsetTable.put(topic + TOPIC_GROUP_SEPARATOR + destGroup, offsets);
        }
    }

}

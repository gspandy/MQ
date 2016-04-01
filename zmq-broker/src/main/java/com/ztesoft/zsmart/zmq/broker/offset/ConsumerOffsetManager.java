package com.ztesoft.zsmart.zmq.broker.offset;

import java.util.Iterator;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.ztesoft.zsmart.zmq.broker.BrokerController;
import com.ztesoft.zsmart.zmq.common.ConfigManager;
import com.ztesoft.zsmart.zmq.common.constant.LoggerName;

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

    private ConcurrentHashMap<String /* topic@group */, ConcurrentHashMap<Integer, Long>> offsetTable = new ConcurrentHashMap<String, ConcurrentHashMap<Integer, Long>>();

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
     * @taskId <br> <br>
     */
    public void scanUnsubscribedTopic(){
        Iterator<Entry<String,ConcurrentHashMap<Integer, Long>>> it = this.offsetTable.entrySet().iterator();
        while(it.hasNext()){
            Entry<String, ConcurrentHashMap<Integer, Long>> next = it.next();
            String topicAtGroup = next.getKey();
            String[] arrays = topicAtGroup.split(TOPIC_GROUP_SEPARATOR);
            if(arrays != null && arrays.length == 2){
                String topic = arrays[0];
                String group = arrays[1];
                
                //当前订阅关系没有group-topic订阅关系 （消费端当前是停机的状态 ）并且offset落后很多 则删除消费进度
                if(null == brokerController.getConsumerManager().findSubscriptionData(group,topic)
                    && this.offsetBehindMuchThanData(topic,next.getValue())){
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
        Iterator<Entry<Integer,Long>> it = table.entrySet().iterator();

        boolean result = !table.isEmpty();
        while(it.hasNext() && result){
            Entry<Integer ,Long > next = it.next();
            long minOffsetInStore = this.brokerController.getMessageStore().getMinOffsetInQuque(topic, next.getKey());
            long offsetInPersist =  next.getValue();
            if(offsetInPersist > minOffsetInStore){
                return false;
            }
        }
        return result;
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

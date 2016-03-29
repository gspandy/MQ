package com.ztesoft.zsmart.zmq.client.impl;

import java.util.Collections;
import java.util.List;

import org.slf4j.Logger;

import com.ztesoft.zsmart.zmq.client.exception.MQClientException;
import com.ztesoft.zsmart.zmq.client.impl.factory.MQClientInstance;
import com.ztesoft.zsmart.zmq.client.log.ClientLogger;
import com.ztesoft.zsmart.zmq.common.MixAll;
import com.ztesoft.zsmart.zmq.common.TopicConfig;
import com.ztesoft.zsmart.zmq.common.protocol.route.BrokerData;
import com.ztesoft.zsmart.zmq.common.protocol.route.TopicRouteData;

public class MQAdminImpl {
    private final Logger log = ClientLogger.getLog();

    private final MQClientInstance mQClientFactory;

    public MQAdminImpl(MQClientInstance mQClientFactory) {
        this.mQClientFactory = mQClientFactory;
    }

    /**
     * 创建topic: <br>
     * 
     * @author wang.jun<br>
     * @taskId <br>
     * @param key
     * @param newTopic
     * @param queueNum
     * @param topicSysFlag
     * @throws MQClientException <br>
     */
    public void createTopic(String key, String newTopic, int queueNum) throws MQClientException {
        createTopic(key, newTopic, queueNum, 0);
    }

    /**
     * 创建topic: <br>
     * 
     * @author wang.jun<br>
     * @taskId <br>
     * @param key
     * @param newTopic
     * @param queueNum
     * @param topicSysFlag
     * @throws MQClientException <br>
     */
    public void createTopic(String key, String newTopic, int queueNum, int topicSysFlag) throws MQClientException {
        TopicRouteData topicRouteData = this.mQClientFactory.getMQClientAPIImpl().getTopicRouteInfoFromNameServer(key,
            1000 * 3);
        try {
            List<BrokerData> brokerDataList = topicRouteData.getBrokerDatas();
            if (brokerDataList != null && !brokerDataList.isEmpty()) {
                Collections.sort(brokerDataList);

                MQClientException exception = null;

                StringBuilder orderTopicString = new StringBuilder();

                for (BrokerData brokerData : brokerDataList) {
                    String addr = brokerData.getBrokerAddrs().get(MixAll.MASTER_ID);
                    if (addr != null) {
                        TopicConfig topicConfig = new TopicConfig(newTopic);
                        topicConfig.setReadQueueNums(queueNum);
                        topicConfig.setWriteQueueNums(queueNum);
                        topicConfig.setTopicSysFlag(topicSysFlag);

                        try {
                            this.mQClientFactory.getMQClientAPIImpl().createTopic(addr, key, topicConfig, 1000 * 3);
                        }
                        catch (Exception e) {
                            exception = new MQClientException("create topic to broker exception", e);
                        }

                        orderTopicString.append(brokerData.getBrokerName());
                        orderTopicString.append(":");
                        orderTopicString.append(queueNum);
                        orderTopicString.append(";");
                    }
                }

                if (exception != null) {
                    throw exception;
                }
            }
            else {
                throw new MQClientException("Not found broker, maybe key is wrong", null);
            }

        }
        catch (Exception e) {
            throw new MQClientException("create new topic failed", e);
        }
    }
}

package com.ztesoft.zsmart.zmq.common.protocol.body;

import java.util.HashSet;
import java.util.Set;

import com.ztesoft.zsmart.zmq.remoting.protocol.RemotingSerializable;

/**
 * TopicList defined <br>
 * 
 * @author wang.jun<br>
 * @version 1.0<br>
 * @taskId <br>
 * @CreateDate 2016年3月21日 <br>
 * @since V7.3<br>
 * @see com.ztesoft.zsmart.zmq.common.protocol.body <br>
 */
public class TopicList extends RemotingSerializable {
    private Set<String> topicList = new HashSet<String>();

    private String brokerAddr;

    public Set<String> getTopicList() {
        return topicList;
    }

    public void setTopicList(Set<String> topicList) {
        this.topicList = topicList;
    }

    public String getBrokerAddr() {
        return brokerAddr;
    }

    public void setBrokerAddr(String brokerAddr) {
        this.brokerAddr = brokerAddr;
    }

}

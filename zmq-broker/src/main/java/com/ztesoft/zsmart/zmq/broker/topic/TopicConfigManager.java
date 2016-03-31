package com.ztesoft.zsmart.zmq.broker.topic;

import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.ztesoft.zsmart.zmq.common.ConfigManager;
import com.ztesoft.zsmart.zmq.common.constant.LoggerName;

public class TopicConfigManager extends ConfigManager {
    private static final Logger log = LoggerFactory.getLogger(LoggerName.BrokerLoggerName);
    private static final long LockTimeoutMillis = 3000;
    private transient final Lock lockTopicConfigTable = new ReentrantLock();
    
    
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

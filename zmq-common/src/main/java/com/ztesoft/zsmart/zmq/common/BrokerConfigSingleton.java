package com.ztesoft.zsmart.zmq.common;

import java.util.concurrent.atomic.AtomicBoolean;

/**
 * 
 * BrokerConfig <br> 
 *  
 * @author wang.jun<br>
 * @version 1.0<br>
 * @taskId <br>
 * @CreateDate 2016年3月21日 <br>
 * @since V7.3<br>
 * @see com.ztesoft.zsmart.zmq.common <br>
 */
public class BrokerConfigSingleton {
    public static AtomicBoolean isInit = new AtomicBoolean();

    private static BrokerConfig brokerConfig;

    public static void setBrokerConfig(BrokerConfig brokerConfig) {
        if (!isInit.compareAndSet(false, true)) {
            throw new IllegalArgumentException("已经初始化过了");
        }
        BrokerConfigSingleton.brokerConfig = brokerConfig;
    }

    public static BrokerConfig getBrokerConfig() {
        if (brokerConfig == null) {
            throw new IllegalArgumentException("没有设置值");
        }
        return brokerConfig;
    }
}

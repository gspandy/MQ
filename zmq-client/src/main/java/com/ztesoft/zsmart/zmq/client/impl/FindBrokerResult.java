package com.ztesoft.zsmart.zmq.client.impl;

/**
 * 
 * 查找Broker结果 <br> 
 *  
 * @author wang.jun<br>
 * @version 1.0<br>
 * @taskId <br>
 * @CreateDate 2016年3月25日 <br>
 * @since V7.3<br>
 * @see com.ztesoft.zsmart.zmq.client.impl <br>
 */
public class FindBrokerResult {
    private final String brokerAddr;

    private final boolean slave;

    public FindBrokerResult(String brokerAddr, boolean slave) {
        this.brokerAddr = brokerAddr;
        this.slave = slave;
    }

    public String getBrokerAddr() {
        return brokerAddr;
    }

    public boolean isSlave() {
        return slave;
    }
}

package com.ztesoft.zsmart.zmq.store.config;

/**
 * 定义Broker角色 <br>
 * 
 * @author wang.jun<br>
 * @version 1.0<br>
 * @taskId <br>
 * @CreateDate 2016年3月22日 <br>
 * @since V7.3<br>
 * @see com.ztesoft.zsmart.zmq.store.config <br>
 */
public enum BrokerRole {
    ASYNC_MASTER, // 异步复制Master

    SYNC_MASTER, // 同步双写Master
    
    SLAVE // Slave
}

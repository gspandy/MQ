package com.ztesoft.zsmart.zmq.common.protocol.heartbeat;

/**
 * 主动消费<br>
 * 
 * @author wang.jun<br>
 * @version 1.0<br>
 * @taskId <br>
 * @CreateDate 2016年3月21日 <br>
 * @since V7.3<br>
 * @see com.ztesoft.zsmart.zmq.common.protocol.heartbeat <br>
 */
public enum ConsumeType {
    /**
     * 主动方式消费
     */
    CONSUME_ACTIVELY,
    /**
     * 被动方式消费
     */
    CONSUME_PASSIVELY
}

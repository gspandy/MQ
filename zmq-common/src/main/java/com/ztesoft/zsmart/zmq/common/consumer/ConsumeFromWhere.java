package com.ztesoft.zsmart.zmq.common.consumer;

/**
 * Consumer 消费位置定义 <br>
 * 
 * @author wang.jun<br>
 * @version 1.0<br>
 * @taskId <br>
 * @CreateDate 2016年3月21日 <br>
 * @since V7.3<br>
 * @see com.ztesoft.zsmart.zmq.common.consumer <br>
 */
public enum ConsumeFromWhere {
    /**
     * 一个新的订阅组第一次启动从队列的最后位置开始消费<br>
     * 后续再启动接着上次消费的进度开始消费
     */
    CONSUME_FROM_LAST_OFFSET,

    @Deprecated
    CONSUME_FROM_LAST_OFFSET_AND_FROM_MIN_WHEN_BOOT_FIRST, @Deprecated
    CONSUME_FROM_MIN_OFFSET, @Deprecated
    CONSUME_FROM_MAX_OFFSET,
    /**
     * 一个新的订阅组第一次启动从队列的最前位置开始消费<br>
     * 后续再启动接着上次消费的进度开始消费
     */
    CONSUME_FROM_FIRST_OFFSET,
    /**
     * 一个新的订阅组第一次启动从指定时间点开始消费<br>
     * 后续再启动接着上次消费的进度开始消费<br>
     * 时间点设置参见DefaultMQPushConsumer.consumeTimestamp参数
     */
    CONSUME_FROM_TIMESTAMP,
}

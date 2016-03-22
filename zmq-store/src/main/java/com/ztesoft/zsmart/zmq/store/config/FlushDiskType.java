package com.ztesoft.zsmart.zmq.store.config;

/**
 * 刷盘方式定义 <br>
 * 
 * @author wang.jun<br>
 * @version 1.0<br>
 * @taskId <br>
 * @CreateDate 2016年3月22日 <br>
 * @since V7.3<br>
 * @see com.ztesoft.zsmart.zmq.store.config <br>
 */
public enum FlushDiskType {
    /**
     * 同步刷盘
     */
    SYNC_FLUSH,
    /**
     * 异步刷盘
     */
    ASYNC_FLUSH
}

package com.ztesoft.zsmart.zmq.client.producer;

/**
 * 本地事务状态 <br>
 * 
 * @author wang.jun<br>
 * @version 1.0<br>
 * @taskId <br>
 * @CreateDate 2016年3月25日 <br>
 * @since V7.3<br>
 * @see com.ztesoft.zsmart.zmq.client.producer <br>
 */
public enum LocalTransactionState {
    COMMIT_MESSAGE, ROLLBACK_MESSAGE, UNKNOW
}

package com.ztesoft.zsmart.zmq.client.producer;

/**
 * 
 * 发送消息回调 <br> 
 *  
 * @author wang.jun<br>
 * @version 1.0<br>
 * @taskId <br>
 * @CreateDate 2016年3月25日 <br>
 * @since V7.3<br>
 * @see com.ztesoft.zsmart.zmq.client.producer <br>
 */
public interface SendCallback {
    public void onSuccess(final SendResult sendResult);
    
    public void onException(final Throwable e);
}

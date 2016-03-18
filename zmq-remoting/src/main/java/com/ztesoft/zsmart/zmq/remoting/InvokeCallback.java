package com.ztesoft.zsmart.zmq.remoting;

import com.ztesoft.zsmart.zmq.remoting.netty.ResponseFuture;

/**
 * 异步调用应答回调接口<br>
 * 
 * @author wang.jun<br>
 * @version 1.0<br>
 * @taskId <br>
 * @CreateDate 2016年3月18日 <br>
 * @since V7.3<br>
 * @see com.ztesoft.zsmart.zmq.remoting <br>
 */
public interface InvokeCallback {
    public void operationComplete(final ResponseFuture responseFuture);
}

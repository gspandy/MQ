package com.ztesoft.zsmart.zmq.remoting;

import com.ztesoft.zsmart.zmq.remoting.exception.RemotingCommandException;

/**
 * 
 * 自定义消息头验证接口 <br> 
 *  
 * @author wang.jun<br>
 * @version 1.0<br>
 * @taskId <br>
 * @CreateDate 2016年3月18日 <br>
 * @since V7.3<br>
 * @see com.ztesoft.zsmart.zmq.remoting.protocol <br>
 */
public interface CommandCustomHeader {
    void checkFields() throws RemotingCommandException;
}

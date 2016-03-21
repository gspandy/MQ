package com.ztesoft.zsmart.zmq.remoting.exception;

/**
 * 
 * 定义通信层异常父类 <br> 
 *  
 * @author wang.jun<br>
 * @version 1.0<br>
 * @taskId <br>
 * @CreateDate 2016年3月18日 <br>
 * @since V7.3<br>
 * @see com.ztesoft.zsmart.zmq.remoting.exception <br>
 */
public class RemotingException extends Exception {

    /**
     * serialVersionUID <br>
     */
    private static final long serialVersionUID = 5720290324284569635L;

    
    public RemotingException(String message){
        super(message);
    }
    
    public RemotingException(String message,Throwable cause){
        super(message,cause);
    }
}

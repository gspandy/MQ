package com.ztesoft.zsmart.zmq.remoting.exception;

/**
 * 
 * PRC 调用中 客户端发送请求失败 抛出此异常 <br> 
 *  
 * @author wang.jun<br>
 * @version 1.0<br>
 * @taskId <br>
 * @CreateDate 2016年3月18日 <br>
 * @since V7.3<br>
 * @see com.ztesoft.zsmart.zmq.remoting.exception <br>
 */
public class RemotingSendRequestException extends RemotingException {

    /**
     * serialVersionUID <br>
     */
    private static final long serialVersionUID = 1L;

    public RemotingSendRequestException(String addr) {
        super(addr,null);
    }
    
    public RemotingSendRequestException(String addr ,Throwable cause) {
        super("send request to <"+addr +"> field",cause);
    }

}

package com.ztesoft.zsmart.zmq.remoting.exception;

/**
 * Client 连接Server失败 抛出异常 <br>
 * 
 * @author wang.jun<br>
 * @version 1.0<br>
 * @taskId <br>
 * @CreateDate 2016年3月18日 <br>
 * @since V7.3<br>
 * @see com.ztesoft.zsmart.zmq.remoting.exception <br>
 */
public class RemotingConnectException extends RemotingException {
    /**
     * serialVersionUID <br>
     */
    private static final long serialVersionUID = 499807592188660864L;

    public RemotingConnectException(String addr) {
        this(addr, null);
    }

    public RemotingConnectException(String addr, Throwable cause) {
        super("connect to <" + addr + "> failed", cause);
    }
}

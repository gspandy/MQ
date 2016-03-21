package com.ztesoft.zsmart.zmq.remoting.exception;

/**
 * 
 * 命令解析自定义字段时 检验字段有效性抛出异常 <br> 
 *  
 * @author wang.jun<br>
 * @version 1.0<br>
 * @taskId <br>
 * @CreateDate 2016年3月18日 <br>
 * @since V7.3<br>
 * @see com.ztesoft.zsmart.zmq.remoting.exception <br>
 */
public class RemotingCommandException extends RemotingException {
    /**
     * serialVersionUID <br>
     */
    private static final long serialVersionUID = 2874871431420572805L;

    public RemotingCommandException(String message) {
        super(message);
    }

    public RemotingCommandException(String message, Throwable cause) {
        super(message, cause);
    }

}

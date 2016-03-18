package com.ztesoft.zsmart.zmq.remoting.protocol;

/**
 * 
 * define remoting response code <br> 
 *  
 * @author wang.jun<br>
 * @version 1.0<br>
 * @taskId <br>
 * @CreateDate 2016年3月18日 <br>
 * @since V7.3<br>
 * @see com.ztesoft.zsmart.zmq.remoting.protocol <br>
 */
public class RemotingSysResponseCode {
    
    //成功
    public static final int SUCCESS = 0;
    
    //发生了未捕获异常
    public static final int SYSTEM_ERROR = 1;
    
    //由于线程池拥堵 系统繁忙
    public static final int SYSTEM_BUSY = 2;
    
    //请求代码不支持
    public static final int REQUEST_CODE_NOT_SUPPORTED = 3;
    
    //事务失败 添加db 失败
    public static final int TRANSACTION_FAILED = 4;
}

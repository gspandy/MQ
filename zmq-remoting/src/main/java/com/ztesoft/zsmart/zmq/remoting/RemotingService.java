package com.ztesoft.zsmart.zmq.remoting;

/**
 * 
 * 定义Remoting接口定义 <br> 
 *  
 * @author wang.jun<br>
 * @version 1.0<br>
 * @taskId <br>
 * @CreateDate 2016年3月18日 <br>
 * @since V7.3<br>
 * @see com.ztesoft.zsmart.zmq.remoting <br>
 */
public interface RemotingService {
    public void start();
    
    public void shutdown();
    
    public void registerRPcHook(RPCHook rpcHook);
}

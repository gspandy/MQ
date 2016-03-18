package com.ztesoft.zsmart.zmq.remoting;

import java.util.concurrent.ExecutorService;

import io.netty.channel.Channel;

import com.ztesoft.zsmart.zmq.remoting.common.Pair;
import com.ztesoft.zsmart.zmq.remoting.exception.RemotingSendRequestException;
import com.ztesoft.zsmart.zmq.remoting.exception.RemotingTimeoutException;
import com.ztesoft.zsmart.zmq.remoting.exception.RemotingTooMuchRequestException;
import com.ztesoft.zsmart.zmq.remoting.netty.NettyRequestProcessor;
import com.ztesoft.zsmart.zmq.remoting.protocol.RemotingCommand;

/**
 * 远程通信 Server 接口<br>
 * 
 * @author wang.jun<br>
 * @version 1.0<br>
 * @taskId <br>
 * @CreateDate 2016年3月18日 <br>
 * @since V7.3<br>
 * @see com.ztesoft.zsmart.zmq.remoting <br>
 */
public interface RemotingServer extends RemotingService {

    /**
     * 注册请求处理器 ExecutorService必须要对对应的一个队列大小有限制的阻塞队列: <br>
     * 
     * @author wang.jun<br>
     * @taskId <br>
     * @param requestCode
     * @param processor
     * @param executor <br>
     */
    public void registerProcessor(final int requestCode, final NettyRequestProcessor processor,
        final ExecutorService executor);

    public void registerDefaultProcessor(final NettyRequestProcessor process, final ExecutorService executor);

    /**
     * 服务绑定本地端口: <br>
     * 
     * @author wang.jun<br>
     * @taskId <br>
     * @return <br>
     */
    public int localListenPort();

    public Pair<NettyRequestProcessor, ExecutorService> getProcessorPair(final int requestCode);

    public RemotingCommand invokeSync(final Channel channel, final RemotingCommand request, final long timeoutMillis)
        throws InterruptedException, RemotingSendRequestException, RemotingTimeoutException;

    public void invokeAsync(final Channel channel, final RemotingCommand request, final long timeoutMillis,
        final InvokeCallback invokeCallback) throws InterruptedException, RemotingTooMuchRequestException,
        RemotingTimeoutException, RemotingSendRequestException;

    public void invokeOneway(final Channel channel, final RemotingCommand request, final long timeoutMillis)
        throws InterruptedException, RemotingTooMuchRequestException, RemotingTimeoutException,
        RemotingSendRequestException;

}

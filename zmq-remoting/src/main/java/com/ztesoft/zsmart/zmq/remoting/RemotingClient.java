package com.ztesoft.zsmart.zmq.remoting;

import java.util.List;
import java.util.concurrent.ExecutorService;

import com.ztesoft.zsmart.zmq.remoting.exception.RemotingConnectException;
import com.ztesoft.zsmart.zmq.remoting.exception.RemotingSendRequestException;
import com.ztesoft.zsmart.zmq.remoting.exception.RemotingTimeoutException;
import com.ztesoft.zsmart.zmq.remoting.exception.RemotingTooMuchRequestException;
import com.ztesoft.zsmart.zmq.remoting.netty.NettyRequestProcessor;
import com.ztesoft.zsmart.zmq.remoting.protocol.RemotingCommand;

/**
 * 远程通信 Client 接口 <br>
 * 
 * @author wang.jun<br>
 * @version 1.0<br>
 * @taskId <br>
 * @CreateDate 2016年3月18日 <br>
 * @since V7.3<br>
 * @see com.ztesoft.zsmart.zmq.remoting <br>
 */
public interface RemotingClient extends RemotingService {
    public void updateNameServerAddressList(final List<String> addrs);

    public List<String> getNameServerAddressList();

    public RemotingCommand invokeSync(final String addr, final RemotingCommand request, final long timeoutMillis)
        throws InterruptedException, RemotingConnectException, RemotingSendRequestException, RemotingTimeoutException;

    public void invokeAsync(final String addr, final RemotingCommand request, final long timeoutMillis,
        final InvokeCallback invokeCallback) throws InterruptedException, RemotingConnectException,
        RemotingSendRequestException, RemotingTimeoutException, RemotingTooMuchRequestException;

    public void invokeOneway(final String addr, final RemotingCommand request, final long timeoutMillis)
        throws InterruptedException, RemotingConnectException, RemotingTooMuchRequestException,
        RemotingTimeoutException, RemotingSendRequestException;

    public void registerProcessor(final int requestCode, final NettyRequestProcessor processor,
        final ExecutorService executor);

    public boolean isChannelWriteable(final String addr);
}

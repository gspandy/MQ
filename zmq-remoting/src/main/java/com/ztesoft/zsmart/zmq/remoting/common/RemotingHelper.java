package com.ztesoft.zsmart.zmq.remoting.common;

import java.net.InetSocketAddress;
import java.net.SocketAddress;

import com.ztesoft.zsmart.zmq.remoting.exception.RemotingConnectException;
import com.ztesoft.zsmart.zmq.remoting.exception.RemotingSendRequestException;
import com.ztesoft.zsmart.zmq.remoting.exception.RemotingTimeoutException;
import com.ztesoft.zsmart.zmq.remoting.protocol.RemotingCommand;

import io.netty.channel.Channel;

/**
 * 定义通信层辅助方法
 * 
 * @author J.Wang
 *
 */
public class RemotingHelper {
	public static final String RemotingLogName = "ZMQRemoting";

	/**
	 * 获取异常信息
	 * 
	 * @param e
	 * @return
	 */
	public static String execptionSimpelDesc(final Throwable e) {
		StringBuffer sb = new StringBuffer();

		if (e != null) {
			sb.append(e.toString());
			StackTraceElement[] stackTrace = e.getStackTrace();
			if (stackTrace != null && stackTrace.length > 0) {
				StackTraceElement element = stackTrace[0];
				sb.append(",");
				sb.append(element.toString());
			}
		}
		return sb.toString();
	}

	/**
	 * IP:PORT to SocketAddress
	 * 
	 * @param addr
	 * @return
	 */
	public static SocketAddress string2SocketAddress(final String addr) {
		String[] s = addr.split(":");
		InetSocketAddress isa = new InetSocketAddress(s[0], Integer.valueOf(s[1]));
		return isa;
	}

	public static RemotingCommand invokeSync(final String addr, final RemotingCommand request, final long timeoutMillis)
			throws InterruptedException, RemotingConnectException, RemotingSendRequestException,
			RemotingTimeoutException {
		long beginTime = System.currentTimeMillis();
	}
	
    public static String parseChannelRemoteAddr(final Channel channel) {
        if (null == channel) {
            return "";
        }
        final SocketAddress remote = channel.remoteAddress();
        final String addr = remote != null ? remote.toString() : "";

        if (addr.length() > 0) {
            int index = addr.lastIndexOf("/");
            if (index >= 0) {
                return addr.substring(index + 1);
            }

            return addr;
        }

        return "";
    }

}

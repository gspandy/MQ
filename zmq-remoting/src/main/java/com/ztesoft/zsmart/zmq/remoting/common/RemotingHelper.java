package com.ztesoft.zsmart.zmq.remoting.common;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;
import java.util.concurrent.TimeUnit;

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
	public static String exceptionSimpleDesc(final Throwable e) {
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

	/*
	 * 异步调用
	 */
	public static RemotingCommand invokeSync(final String addr, final RemotingCommand request, final long timeoutMillis)
			throws InterruptedException, RemotingConnectException, RemotingSendRequestException,
			RemotingTimeoutException {
		long beginTime = System.currentTimeMillis();
		SocketAddress socketAddress = RemotingHelper.string2SocketAddress(addr);
		SocketChannel socketChannel = RemotingUtil.connect(socketAddress);

		if (socketChannel != null) {
			boolean sendRequestOK = false;
			try {
				// 使用阻塞模式
				socketChannel.configureBlocking(true);
				socketChannel.socket().setSoTimeout((int) timeoutMillis);

				// 发送数据
				ByteBuffer byteBufferRequest = request.encode();
				while (byteBufferRequest.hasRemaining()) {
					int length = socketChannel.write(byteBufferRequest);
					if (length > 0) {
						if (byteBufferRequest.hasRemaining()) {
							if ((System.currentTimeMillis() - beginTime) > timeoutMillis) {
								// 发送请求超时
								throw new RemotingSendRequestException(addr);
							}
						}
					} else {
						throw new RemotingSendRequestException(addr);
					}

					TimeUnit.MILLISECONDS.sleep(1);
				}
				sendRequestOK = true;

				// 接收应答SIZE
				ByteBuffer byteBufferSize = ByteBuffer.allocate(4);
				while (byteBufferSize.hasRemaining()) {
					int length = socketChannel.read(byteBufferSize);
					if (length > 0) {
						if (byteBufferSize.hasRemaining()) {
							if ((System.currentTimeMillis() - beginTime) > timeoutMillis) {
								// 发送请求超时
								throw new RemotingTimeoutException(addr);
							}
						}
					} else {
						throw new RemotingTimeoutException(addr);
					}
					TimeUnit.MILLISECONDS.sleep(1);
				}

				// 对应答数据进行解码
				byteBufferSize.flip();
				return RemotingCommand.decode(byteBufferSize);

			} catch (IOException e) {
				e.printStackTrace();

				if (sendRequestOK) {
					throw new RemotingTimeoutException(addr, timeoutMillis);
				} else {
					throw new RemotingSendRequestException(addr);
				}
			} finally {
				try {
					socketChannel.close();
				} catch (IOException e) {
					e.printStackTrace();
				}
			}
		} else {
			throw new RemotingConnectException(addr);
		}
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

	/**
	 * 获取远程主机名
	 * 
	 * @param channel
	 * @return
	 */
	public static String parseChannelRemoteName(final Channel channel) {
		if (channel == null) {
			return "";
		}

		final InetSocketAddress remote = (InetSocketAddress) channel.remoteAddress();
		if (remote != null) {
			return remote.getAddress().getHostName();
		}
		return "";
	}

	public static String parseSocketAddressName(SocketAddress socketAddress) {
		final InetSocketAddress addrs = (InetSocketAddress) socketAddress;

		if (addrs != null) {
			return addrs.getAddress().getHostName();
		}

		return "";
	}

}

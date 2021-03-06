package com.ztesoft.zsmart.zmq.remoting.netty;

/**
 * Netty 服务端配置
 * 
 * @author J.Wang
 *
 */
public class NettyServerConfig {
	private int listenPort = 8888;
	private int serverWorkThreads = 8;
	private int serverCallbackExecutorThreads = 0;
	private int serverSelectorThreads = 3;
	private int serverOnewaySemaphoreValue = 256;
	private int serverAsyncSemaphoreValue = 64;
	private int serverChannelMaxIdleTimeSeconds = 120;

	private int serverSocketSndBufSize = NettySystemConfig.SocketSndbufSize;
	private int serverSocketRcvBufSize = NettySystemConfig.SocketRcvbufSize;
	private boolean serverPooledByteBufAllocatorEnable = false;

	public int getListenPort() {
		return listenPort;
	}

	public void setListenPort(int listenPort) {
		this.listenPort = listenPort;
	}

	public int getServerWorkThreads() {
		return serverWorkThreads;
	}

	public void setServerWorkThreads(int serverWorkThreads) {
		this.serverWorkThreads = serverWorkThreads;
	}

	public int getServerCallbackExecutorThreads() {
		return serverCallbackExecutorThreads;
	}

	public void setServerCallbackExecutorThreads(int serverCallbackExecutorThreads) {
		this.serverCallbackExecutorThreads = serverCallbackExecutorThreads;
	}

	public int getServerSelectorThreads() {
		return serverSelectorThreads;
	}

	public void setServerSelectorThreads(int serverSelectorThreads) {
		this.serverSelectorThreads = serverSelectorThreads;
	}

	public int getServerOnewaySemaphoreValue() {
		return serverOnewaySemaphoreValue;
	}

	public void setServerOnewaySemaphoreValue(int serverOnewaySemaphoreValue) {
		this.serverOnewaySemaphoreValue = serverOnewaySemaphoreValue;
	}

	public int getServerAsyncSemaphoreValue() {
		return serverAsyncSemaphoreValue;
	}

	public void setServerAsyncSemaphoreValue(int serverAsyncSemaphoreValue) {
		this.serverAsyncSemaphoreValue = serverAsyncSemaphoreValue;
	}

	public int getServerChannelMaxIdleTimeSeconds() {
		return serverChannelMaxIdleTimeSeconds;
	}

	public void setServerChannelMaxIdleTimeSeconds(int serverChannelMaxIdleTimeSeconds) {
		this.serverChannelMaxIdleTimeSeconds = serverChannelMaxIdleTimeSeconds;
	}

	public int getServerSocketSndBufSize() {
		return serverSocketSndBufSize;
	}

	public void setServerSocketSndBufSize(int serverSocketSndBufSize) {
		this.serverSocketSndBufSize = serverSocketSndBufSize;
	}

	public int getServerSocketRcvBufSize() {
		return serverSocketRcvBufSize;
	}

	public void setServerSocketRcvBufSize(int serverSocketRcvBufSize) {
		this.serverSocketRcvBufSize = serverSocketRcvBufSize;
	}

	public boolean isServerPooledByteBufAllocatorEnable() {
		return serverPooledByteBufAllocatorEnable;
	}

	public void setServerPooledByteBufAllocatorEnable(boolean serverPooledByteBufAllocatorEnable) {
		this.serverPooledByteBufAllocatorEnable = serverPooledByteBufAllocatorEnable;
	}

}

package com.ztesoft.zsmart.zmq.remoting.netty;

import io.netty.channel.Channel;

/**
 * Netty 事件定义
 * 
 * @author J.Wang
 *
 */
public class NettyEvent {
	private final NettyEventType type;
	private final String remoteAddr;
	private final Channel channel;

	public NettyEvent(NettyEventType type, String remoteAddr, Channel channel) {
		this.type = type;
		this.remoteAddr = remoteAddr;
		this.channel = channel;
	}

	public NettyEventType getType() {
		return type;
	}

	public String getRemoteAddr() {
		return remoteAddr;
	}

	public Channel getChannel() {
		return channel;
	}

	public String toString() {
		return "NettyEvent [type=" + type + ", remoteAddr=" + remoteAddr + ", channel=" + channel + "]";
	}
}

package com.ztesoft.zsmart.zmq.common.utils;

import java.net.InetAddress;
import java.net.InetSocketAddress;

import io.netty.channel.Channel;

public class ChannelUtil {
	public static String getRemoteIp(Channel channel) {
		InetSocketAddress inetSocketAddress = (InetSocketAddress) channel.remoteAddress();
		if (inetSocketAddress == null) {
			return "";
		}

		final InetAddress inetAddress = inetSocketAddress.getAddress();
		return (inetAddress != null ? inetAddress.getHostAddress() : inetSocketAddress.getHostName());
	}
}

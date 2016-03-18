package com.ztesoft.zsmart.zmq.remoting.common;

import java.io.IOException;
import java.lang.reflect.Method;
import java.net.Inet6Address;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.NetworkInterface;
import java.net.SocketAddress;
import java.net.SocketException;
import java.net.UnknownHostException;
import java.nio.channels.Selector;
import java.nio.channels.SocketChannel;
import java.nio.channels.spi.SelectorProvider;
import java.util.ArrayList;
import java.util.Enumeration;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelFutureListener;


public class RemotingUtil {
	private static final Logger log = LoggerFactory.getLogger(RemotingUtil.class);
	private static final String OS_NAME = System.getProperty("os.name");

	private static boolean isLinuxPlatform = false;
	private static boolean isWindowsPlatform = false;

	static {
		if (OS_NAME != null && OS_NAME.toLowerCase().indexOf("linux") >= 0) {
			isLinuxPlatform = true;
		}

		if (OS_NAME != null && OS_NAME.toLowerCase().indexOf("windows") >= 0) {
			isWindowsPlatform = true;
		}
	}

	public static boolean isLinuxPlatform() {
		return isLinuxPlatform;
	}

	public static boolean isWindowsPlatform() {
		return isWindowsPlatform;
	}

	public static Selector openSelector() throws IOException {
		Selector result = null;
		// 在linux平台 尽量使用epool现实
		if (isLinuxPlatform) {
			try {
				final Class<?> providerClazz = Class.forName("sun.nio.ch.EPollSelectorProvider");
				if (providerClazz != null) {
					final Method method = providerClazz.getMethod("provider");
					if (method != null) {
						final SelectorProvider selectorProvider = (SelectorProvider) method.invoke(null);
						if (selectorProvider != null) {
							result = selectorProvider.openSelector();
						}
					}
				}

			} catch (Exception e) {
			}
		}
		if (result == null) {
			result = Selector.open();
		}
		return result;
	}

	/**
	 * 获取本机ip
	 * 
	 * @return
	 */
	public static String getLocalAddress() {
		try {
			Enumeration<NetworkInterface> enumeration = NetworkInterface.getNetworkInterfaces();
			ArrayList<String> ipv4Result = new ArrayList<String>();
			ArrayList<String> ipv6Result = new ArrayList<String>();

			while (enumeration.hasMoreElements()) {
				final NetworkInterface networkInterface = enumeration.nextElement();
				final Enumeration<InetAddress> en = networkInterface.getInetAddresses();
				while (en.hasMoreElements()) {
					final InetAddress address = en.nextElement();
					if (!address.isLoopbackAddress()) {
						if (address instanceof Inet6Address) {
							ipv6Result.add(normalizeHostAddress(address));
						} else {
							ipv4Result.add(normalizeHostAddress(address));
						}
					}
				}

			}

			if (!ipv4Result.isEmpty()) {
				for (String ip : ipv4Result) {
					if (ip.startsWith("127.0") || ip.startsWith("192.168")) {
						continue;
					}
					return ip;
				}
				return ipv4Result.get(ipv4Result.size() - 1);
			} else if (!ipv6Result.isEmpty()) {
				return ipv6Result.get(0);
			}

			// If failed to find,fall back to localhost
			final InetAddress localHost = InetAddress.getLocalHost();
			return normalizeHostAddress(localHost);

		} catch (SocketException e) {
			e.printStackTrace();
		} catch (UnknownHostException e) {
			e.printStackTrace();
		}
		return null;

	}

	public static String normalizeHostAddress(final InetAddress localHost) {
		if (localHost instanceof Inet6Address) {
			return "[" + localHost.getHostAddress() + "]";
		} else {
			return localHost.getHostAddress();
		}
	}

    /**
     * IP:PORT
     */
    public static SocketAddress string2SocketAddress(final String addr) {
        String[] s = addr.split(":");
        InetSocketAddress isa = new InetSocketAddress(s[0], Integer.valueOf(s[1]));
        return isa;
    }
    
    public static String socketAddress2String(final SocketAddress address){
    	StringBuilder sb = new StringBuilder();
    	InetSocketAddress inetSocketAddress = (InetSocketAddress) address;
    	sb.append(inetSocketAddress.getAddress().getHostAddress());
    	sb.append(":");
    	sb.append(inetSocketAddress.getPort());
    	return sb.toString();
    }
    
    public static SocketChannel connect(SocketAddress remote){
    	return connect(remote,1000*5);
    }
    
    /**
     * 打开socket channel
     * @param remote
     * @param timeoutMilles
     * @return
     */
    public static SocketChannel connect(SocketAddress remote,final int timeoutMilles){
    	SocketChannel sc = null;
    	
    	try {
			sc = SocketChannel.open();
			sc.configureBlocking(true);
			sc.socket().setSoLinger(false, -1);
			sc.socket().setTcpNoDelay(true);
			sc.socket().setReceiveBufferSize(1024 * 64);
			sc.socket().setSendBufferSize(1024 * 64);
			sc.socket().connect(remote, timeoutMilles);
			sc.configureBlocking(false);
			return sc;
		} catch (Exception e) {
			if(sc != null){
				try {
					sc.close();
				} catch (IOException e1) {
					e1.printStackTrace();
				}
			}
		}
    	
    	return null;
    }
    
    public static void closeChannel(Channel channel) {
        final String addrRemote = RemotingHelper.parseChannelRemoteAddr(channel);
        channel.close().addListener(new ChannelFutureListener() {
            @Override
            public void operationComplete(ChannelFuture future) throws Exception {
                log.info("closeChannel: close the connection to remote address[{}] result: {}", addrRemote,
                    future.isSuccess());
            }
        });
    }
}

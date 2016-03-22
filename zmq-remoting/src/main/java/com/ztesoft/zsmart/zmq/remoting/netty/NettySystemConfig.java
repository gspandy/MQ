package com.ztesoft.zsmart.zmq.remoting.netty;

public class NettySystemConfig {

    public static final String SystemPropertyNettyPoolByteBufferAllocatorEnable = "com.zmq.remoting.nettyPolledByteBufAllocatorEnable";

    public static final boolean NettyPooledByteBufAllocatorEnable = Boolean.parseBoolean(System.getProperty(
        SystemPropertyNettyPoolByteBufferAllocatorEnable, "false"));

    public static final String SystemPropertySocketSndbufSize = "com.zmq.remoting.socket.sndbuf.size";

    public static int SocketSndbufSize = Integer.parseInt(System.getProperty(SystemPropertySocketSndbufSize, "65535"));

    public static final String SystemPropertySocketRcvbufSize = "com.zmq.remoting.socket.rcv.buf.size";

    public static int SocketRcvbufSize = Integer.parseInt(System.getProperty(SystemPropertySocketRcvbufSize, "65535"));

    public static final String SystemPropertyClientAsyncSemaphoreValue = "com.zmq.remoting.clientAsyncSemaphoreValue";

    public static int ClientAsyncSemaphoreValue = Integer.parseInt(System.getProperty(
        SystemPropertyClientAsyncSemaphoreValue, "2048"));

    public static final String SystemPropertyClientOnewaySemaphoreValue = "com.zmq.remoting.clientOnewaySemaphoreValue";

    public static int ClientOnewaySemaphoreValue = Integer.parseInt(System.getProperty(
        SystemPropertyClientOnewaySemaphoreValue, "2048"));

}

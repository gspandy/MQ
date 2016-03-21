package com.ztesoft.zsmart.zmq.remoting.netty;

import java.nio.ByteBuffer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.ztesoft.zsmart.zmq.remoting.common.RemotingHelper;
import com.ztesoft.zsmart.zmq.remoting.common.RemotingUtil;
import com.ztesoft.zsmart.zmq.remoting.protocol.RemotingCommand;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.LengthFieldBasedFrameDecoder;

public class NettyDecoder extends LengthFieldBasedFrameDecoder {
	private static final Logger log = LoggerFactory.getLogger(RemotingHelper.RemotingLogName);
	private static final int FRAME_MAX_LENGTH = //
	Integer.parseInt(System.getProperty("com.zmq.remoting.frameMaxLength", "8388608"));

	public NettyDecoder() {
		super(FRAME_MAX_LENGTH, 0, 4, 0, 4);
	}

	@Override
	protected Object decode(ChannelHandlerContext ctx, ByteBuf in) throws Exception {

		ByteBuf frame = null;
		try {
			frame = (ByteBuf) super.decode(ctx, in);
			if (null == frame) {
				return null;
			}

			ByteBuffer byteBuffer = frame.nioBuffer();

			return RemotingCommand.decode(byteBuffer);
		} catch (Exception e) {
			log.error("decode exception, " + RemotingHelper.parseChannelRemoteAddr(ctx.channel()), e);
			RemotingUtil.closeChannel(ctx.channel());
		} finally {
			if (null != frame) {
				frame.release();
			}
		}

		return null;
	}

}

package com.ztesoft.zsmart.zmq.remoting.netty;

import java.nio.ByteBuffer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.ztesoft.zsmart.zmq.remoting.common.RemotingHelper;
import com.ztesoft.zsmart.zmq.remoting.common.RemotingUtil;
import com.ztesoft.zsmart.zmq.remoting.protocol.RemotingCommand;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.MessageToByteEncoder;

public class NettyEncoder extends MessageToByteEncoder<RemotingCommand> {

	private static final Logger log = LoggerFactory.getLogger(RemotingHelper.RemotingLogName);

	@Override
	protected void encode(ChannelHandlerContext ctx, RemotingCommand remotingCommand, ByteBuf out) throws Exception {
		try {
			ByteBuffer header = remotingCommand.encodeHeader();
			out.writeBytes(header);

			byte[] body = remotingCommand.getBody();
			if (body != null) {
				out.writeBytes(body);
			}
		} catch (Exception e) {
			log.error("encode exceptionn " + RemotingHelper.exceptionSimpleDesc(e));

			if (remotingCommand != null) {
				log.error(remotingCommand.toString());
			}
			RemotingUtil.closeChannel(ctx.channel());
		}

	}

}

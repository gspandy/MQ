package com.ztesoft.zsmart.zmq.namesrv.processor;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.ztesoft.zsmart.zmq.common.constant.LoggerName;
import com.ztesoft.zsmart.zmq.common.protocol.RequestCode;
import com.ztesoft.zsmart.zmq.common.protocol.ResponseCode;
import com.ztesoft.zsmart.zmq.common.protocol.header.namesrv.PutKVConfigRequestHeader;
import com.ztesoft.zsmart.zmq.namesrv.NamesrvController;
import com.ztesoft.zsmart.zmq.remoting.common.RemotingHelper;
import com.ztesoft.zsmart.zmq.remoting.exception.RemotingCommandException;
import com.ztesoft.zsmart.zmq.remoting.netty.NettyRequestProcessor;
import com.ztesoft.zsmart.zmq.remoting.protocol.RemotingCommand;

import io.netty.channel.ChannelHandlerContext;

public class DefaultRequestProcessor implements NettyRequestProcessor {
	private static final Logger log = LoggerFactory.getLogger(LoggerName.NamesrvLoggerName);

	private final NamesrvController namesrvController;

	public DefaultRequestProcessor(NamesrvController namesrvController) {
		this.namesrvController = namesrvController;
	}

	@Override
	public RemotingCommand processRequest(ChannelHandlerContext ctx, RemotingCommand request) throws Exception {

		if (log.isDebugEnabled()) {
			log.debug("receive request {} {} {}", request.getCode(),
					RemotingHelper.parseChannelRemoteAddr(ctx.channel()), request);
		}

		switch (request.getCode()) {
		case RequestCode.PUT_KV_CONFIG:
			return this.putKVConfig(ctx, request);

		default:
			break;
		}

		return null;
	}

	private RemotingCommand putKVConfig(ChannelHandlerContext ctx, RemotingCommand request)
			throws RemotingCommandException {
		final RemotingCommand response = RemotingCommand.createResponseCommand(null);

		final PutKVConfigRequestHeader requestHeader = (PutKVConfigRequestHeader) request
				.decodeCommandCustomHeader(PutKVConfigRequestHeader.class);
		this.namesrvController.getKvConfigManager().putKVConfig(requestHeader.getNamespace(), requestHeader.getKey(),
				requestHeader.getValue());

		response.setCode(ResponseCode.SUCCESS);
		response.setRemark(null);
		return response;
	}

}

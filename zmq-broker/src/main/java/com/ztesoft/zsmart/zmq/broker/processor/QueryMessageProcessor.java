package com.ztesoft.zsmart.zmq.broker.processor;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.ztesoft.zsmart.zmq.broker.BrokerController;
import com.ztesoft.zsmart.zmq.broker.pagecache.OneMessageTransfer;
import com.ztesoft.zsmart.zmq.broker.pagecache.QueryMessageTransfer;
import com.ztesoft.zsmart.zmq.common.constant.LoggerName;
import com.ztesoft.zsmart.zmq.common.protocol.RequestCode;
import com.ztesoft.zsmart.zmq.common.protocol.ResponseCode;
import com.ztesoft.zsmart.zmq.common.protocol.header.QueryMessageRequestHeader;
import com.ztesoft.zsmart.zmq.common.protocol.header.QueryMessageResponseHeader;
import com.ztesoft.zsmart.zmq.common.protocol.header.ViewMessageRequestHeader;
import com.ztesoft.zsmart.zmq.remoting.exception.RemotingCommandException;
import com.ztesoft.zsmart.zmq.remoting.netty.NettyRequestProcessor;
import com.ztesoft.zsmart.zmq.remoting.protocol.RemotingCommand;
import com.ztesoft.zsmart.zmq.store.QueryMessageResult;
import com.ztesoft.zsmart.zmq.store.SelectMapedBufferResult;

import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.FileRegion;


/**
 * 查询消息请求处理 <br>
 * 
 * @author wang.jun<br>
 * @version 1.0<br>
 * @taskId <br>
 * @CreateDate 2016年4月5日 <br>
 * @since V7.3<br>
 * @see com.ztesoft.zsmart.zmq.broker.processor <br>
 */
public class QueryMessageProcessor implements NettyRequestProcessor {

    private static final Logger log = LoggerFactory.getLogger(LoggerName.BrokerLoggerName);

    private final BrokerController brokerController;


    public QueryMessageProcessor(final BrokerController brokerController) {
        this.brokerController = brokerController;
    }


    @Override
    public RemotingCommand processRequest(ChannelHandlerContext ctx, RemotingCommand request)
            throws RemotingCommandException {

        switch (request.getCode()) {
        case RequestCode.QUERY_MESSAGE:
            return this.queryMessage(ctx, request);
        case RequestCode.VIEW_MESSAGE_BY_ID:
            return this.viewMessageById(ctx, request);
        default:
            break;
        }

        return null;
    }


    /**
     * Description: <br>
     * 
     * @author wang.jun<br>
     * @taskId <br>
     * @param ctx
     * @param request
     * @return <br>
     * @throws RemotingCommandException
     */
    private RemotingCommand queryMessage(ChannelHandlerContext ctx, RemotingCommand request)
            throws RemotingCommandException {
        final RemotingCommand response =
                RemotingCommand.createResponseCommand(QueryMessageResponseHeader.class);

        final QueryMessageResponseHeader responseHeader =
                (QueryMessageResponseHeader) response.readCustomHeader();

        final QueryMessageRequestHeader requestHeader = (QueryMessageRequestHeader) request
            .decodeCommandCustomHeader(QueryMessageRequestHeader.class);

        // 由于使用sendfile 所以必须要设置
        response.setOpaque(request.getOpaque());

        final QueryMessageResult queryMessageResult = this.brokerController.getMessageStore().queryMessage(
            requestHeader.getTopic(), requestHeader.getKey(), requestHeader.getMaxNum(),
            requestHeader.getBeginTimestamp(), requestHeader.getEndTimestamp());
        assert queryMessageResult != null;

        responseHeader.setIndexLastUpdatePhyoffset(queryMessageResult.getIndexLastUpdatePhyoffset());
        responseHeader.setIndexLastUpdateTimestamp(queryMessageResult.getIndexLastUpdateTimestamp());

        if (queryMessageResult.getBufferTotalSize() > 0) {
            response.setCode(ResponseCode.SUCCESS);
            response.setRemark(null);
            try {
                FileRegion fileRegion = new QueryMessageTransfer(
                    response.encodeHeader(queryMessageResult.getBufferTotalSize()), queryMessageResult);

                ctx.channel().writeAndFlush(fileRegion).addListener(new ChannelFutureListener() {
                    @Override
                    public void operationComplete(ChannelFuture future) throws Exception {
                        queryMessageResult.release();
                        if (!future.isSuccess()) {
                            log.error("transfer query message by pagecache failed, ", future.cause());
                        }
                    }
                });

            }
            catch (Throwable e) {
                log.error("", e);
                queryMessageResult.release();
            }

            return null;
        }

        response.setCode(ResponseCode.QUERY_NOT_FOUND);
        response.setRemark("can not find message, maybe time range not correct");
        return response;
    }


    public RemotingCommand viewMessageById(ChannelHandlerContext ctx, RemotingCommand request)
            throws RemotingCommandException {
        final RemotingCommand response = RemotingCommand.createResponseCommand(null);
        final ViewMessageRequestHeader requestHeader =
                (ViewMessageRequestHeader) request.decodeCommandCustomHeader(ViewMessageRequestHeader.class);

        // 由于使用sendfile，所以必须要设置
        response.setOpaque(request.getOpaque());

        final SelectMapedBufferResult selectMapedBufferResult =
                this.brokerController.getMessageStore().selectOneMessageByOffset(requestHeader.getOffset());
        if (selectMapedBufferResult != null) {
            response.setCode(ResponseCode.SUCCESS);
            response.setRemark(null);

            try {
                FileRegion fileRegion = new OneMessageTransfer(
                    response.encodeHeader(selectMapedBufferResult.getSize()), selectMapedBufferResult);
                ctx.channel().writeAndFlush(fileRegion).addListener(new ChannelFutureListener() {
                    @Override
                    public void operationComplete(ChannelFuture future) throws Exception {
                        selectMapedBufferResult.release();
                        if (!future.isSuccess()) {
                            log.error("transfer one message by pagecache failed, ", future.cause());
                        }
                    }
                });
            }
            catch (Throwable e) {
                log.error("", e);
                selectMapedBufferResult.release();
            }

            return null;
        }
        else {
            response.setCode(ResponseCode.SYSTEM_ERROR);
            response.setRemark("can not find message by the offset, " + requestHeader.getOffset());
        }

        return response;
    }

}

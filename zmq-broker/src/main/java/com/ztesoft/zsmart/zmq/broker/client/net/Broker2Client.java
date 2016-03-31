package com.ztesoft.zsmart.zmq.broker.client.net;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.FileRegion;
import io.netty.channel.Channel;

import com.ztesoft.zsmart.zmq.broker.BrokerController;
import com.ztesoft.zsmart.zmq.broker.pagecache.OneMessageTransfer;
import com.ztesoft.zsmart.zmq.common.TopicConfig;
import com.ztesoft.zsmart.zmq.common.constant.LoggerName;
import com.ztesoft.zsmart.zmq.common.protocol.RequestCode;
import com.ztesoft.zsmart.zmq.common.protocol.header.NotifyConsumerIdsChangedRequestHeader;
import com.ztesoft.zsmart.zmq.common.protocol.header.namesrv.CheckTransactionStateRequestHeader;
import com.ztesoft.zsmart.zmq.remoting.exception.RemotingSendRequestException;
import com.ztesoft.zsmart.zmq.remoting.exception.RemotingTimeoutException;
import com.ztesoft.zsmart.zmq.remoting.protocol.RemotingCommand;
import com.ztesoft.zsmart.zmq.store.SelectMapedBufferResult;

/**
 * Broker主动调用客户端接口 <br>
 * 
 * @author wang.jun<br>
 * @version 1.0<br>
 * @taskId <br>
 * @CreateDate 2016年3月31日 <br>
 * @since V7.3<br>
 * @see com.ztesoft.zsmart.zmq.broker.client.net <br>
 */
public class Broker2Client {
    private static final Logger log = LoggerFactory.getLogger(LoggerName.BrokerLoggerName);

    private final BrokerController brokerController;

    public Broker2Client(BrokerController brokerController) {
        this.brokerController = brokerController;
    }

    /**
     * Broker 主动回查Producer事务状态 Oneway: <br>
     * 
     * @author wang.jun<br>
     * @taskId <br>
     * <br>
     */
    public void checkProducerTransactionState(//
        final Channel channel, //
        final CheckTransactionStateRequestHeader requestHeader,//
        final SelectMapedBufferResult selectMapedBufferResult//
    ) {
        RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.CHECK_TRANSACTION_STATE,
            requestHeader);

        request.markOnewayRPC();
        try {
            FileRegion fileRegion = new OneMessageTransfer(request.encodeHeader(selectMapedBufferResult.getSize()),
                selectMapedBufferResult);

            channel.writeAndFlush(fileRegion).addListener(new ChannelFutureListener() {

                @Override
                public void operationComplete(ChannelFuture future) throws Exception {
                    selectMapedBufferResult.release();
                    if (!future.isSuccess()) {
                        log.error("invokeProducer failed,", future.cause());
                    }
                }
            });
        }
        catch (Throwable e) {
            log.error("invokeProducer exception", e);
            selectMapedBufferResult.release();
        }
    }

    /**
     * 同步调用客户端: <br>
     * 
     * @author wang.jun<br>
     * @taskId <br>
     * @param channel
     * @param request
     * @return
     * @throws RemotingSendRequestException
     * @throws RemotingTimeoutException
     * @throws InterruptedException <br>
     */
    public RemotingCommand callClient(//
        final Channel channel,//
        final RemotingCommand request) throws RemotingSendRequestException, RemotingTimeoutException,
        InterruptedException {
        return this.brokerController.getRemotingServer().invokeSync(channel, request, 10000);
    }

    /**
     * Broker主动通知Consumer id列表发生变化: <br>
     * 
     * @author wang.jun<br>
     * @taskId <br>
     * @param channel
     * @param consumerGroup <br>
     */
    public void notifyConsumerIdsChanged(//
        final Channel channel,//
        final String consumerGroup) {
        if (consumerGroup == null) {
            log.error("notifyConsumerIdsChanged consumerGroup is null");
            return;
        }

        NotifyConsumerIdsChangedRequestHeader requestHeader = new NotifyConsumerIdsChangedRequestHeader();

        requestHeader.setConsumerGroup(consumerGroup);

        RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.NOTIFY_CONSUMER_IDS_CHANGED,
            requestHeader);

        try {
            this.brokerController.getRemotingServer().invokeOneway(channel, request, 10);
        }
        catch (Exception e) {
            log.error("notifyConsumerIdsChanged exception, " + consumerGroup, e);
        }
    }
    
    public RemotingCommand resetOffset(String topic,String group,long timeStamp,boolean isForce){
        final RemotingCommand reponse = RemotingCommand.createResponseCommand(null);
        TopicConfig topicConfig = this.brokerController.gett
        
    }
    
    
    
    
    
    
    
}

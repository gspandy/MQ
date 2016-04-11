package com.ztesoft.zsmart.zmq.client.impl;

import java.nio.ByteBuffer;

import org.slf4j.Logger;

import io.netty.channel.ChannelHandlerContext;

import com.ztesoft.zsmart.zmq.client.impl.factory.MQClientInstance;
import com.ztesoft.zsmart.zmq.client.impl.producer.MQProducerInner;
import com.ztesoft.zsmart.zmq.client.log.ClientLogger;
import com.ztesoft.zsmart.zmq.common.message.MessageConst;
import com.ztesoft.zsmart.zmq.common.message.MessageDecoder;
import com.ztesoft.zsmart.zmq.common.message.MessageExt;
import com.ztesoft.zsmart.zmq.common.protocol.RequestCode;
import com.ztesoft.zsmart.zmq.common.protocol.header.CheckTransactionStateRequestHeader;
import com.ztesoft.zsmart.zmq.remoting.exception.RemotingCommandException;
import com.ztesoft.zsmart.zmq.remoting.netty.NettyRequestProcessor;
import com.ztesoft.zsmart.zmq.remoting.protocol.RemotingCommand;

/**
 * 客户端通信处理程序 <br>
 * 
 * @author wang.jun<br>
 * @version 1.0<br>
 * @taskId <br>
 * @CreateDate 2016年3月29日 <br>
 * @since V7.3<br>
 * @see com.ztesoft.zsmart.zmq.client.impl <br>
 */
public class ClientRemotingProcessor implements NettyRequestProcessor {
    private final Logger log = ClientLogger.getLog();

    private final MQClientInstance mqClientFactory;

    public ClientRemotingProcessor(final MQClientInstance mqClientFactory) {
        this.mqClientFactory = mqClientFactory;
    }

    @Override
    public RemotingCommand processRequest(ChannelHandlerContext ctx, RemotingCommand request)
        throws RemotingCommandException {
        switch (request.getCode()) {
            case RequestCode.CHECK_TRANSACTION_STATE: // Broker 主动向Producer回查事务状态
                return this.checkTransactionState(ctx, request);

            default:
                break;
        }
        return null;
    }

    /**
     * Broker 主动向Producer回查事务状态 Description: <br>
     * 
     * @author wang.jun<br>
     * @taskId <br>
     * @return <br>
     * @throws RemotingCommandException 
     */
    private RemotingCommand checkTransactionState(ChannelHandlerContext ctx, RemotingCommand request) throws RemotingCommandException {
        final CheckTransactionStateRequestHeader requestHeader = (CheckTransactionStateRequestHeader) request
            .decodeCommandCustomHeader(CheckTransactionStateRequestHeader.class);
        
        final ByteBuffer buffer =  ByteBuffer.wrap(request.getBody());
        final MessageExt message = MessageDecoder.decode(buffer);
        if(message != null){
            final String group = message.getProperty(MessageConst.PROPERTY_PRODUCER_GROUP);
            if(group != null){
                MQProducerInner producerInner = this.mqClientFactory.set
            }
        }
        
        return null;
    }

}

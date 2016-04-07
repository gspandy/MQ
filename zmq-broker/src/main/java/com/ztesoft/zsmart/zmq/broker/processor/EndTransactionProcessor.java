package com.ztesoft.zsmart.zmq.broker.processor;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.ztesoft.zsmart.zmq.broker.BrokerController;
import com.ztesoft.zsmart.zmq.common.TopicFilterType;
import com.ztesoft.zsmart.zmq.common.constant.LoggerName;
import com.ztesoft.zsmart.zmq.common.message.MessageAccessor;
import com.ztesoft.zsmart.zmq.common.message.MessageConst;
import com.ztesoft.zsmart.zmq.common.message.MessageDecoder;
import com.ztesoft.zsmart.zmq.common.message.MessageExt;
import com.ztesoft.zsmart.zmq.common.protocol.ResponseCode;
import com.ztesoft.zsmart.zmq.common.protocol.header.EndTransactionRequestHeader;
import com.ztesoft.zsmart.zmq.common.sysflag.MessageSysFlag;
import com.ztesoft.zsmart.zmq.remoting.common.RemotingHelper;
import com.ztesoft.zsmart.zmq.remoting.exception.RemotingCommandException;
import com.ztesoft.zsmart.zmq.remoting.netty.NettyRequestProcessor;
import com.ztesoft.zsmart.zmq.remoting.protocol.RemotingCommand;
import com.ztesoft.zsmart.zmq.store.MessageExtBrokerInner;
import com.ztesoft.zsmart.zmq.store.MessageStore;
import com.ztesoft.zsmart.zmq.store.PutMessageResult;

import io.netty.channel.ChannelHandlerContext;


/**
 * Commit或Rollback事务
 * 
 * @author J.Wang
 *
 */
public class EndTransactionProcessor implements NettyRequestProcessor {

    private static final Logger logTransaction = LoggerFactory.getLogger(LoggerName.TransactionLoggerName);

    private final BrokerController brokerController;


    public EndTransactionProcessor(final BrokerController brokerController) {
        this.brokerController = brokerController;
    }


    @Override
    public RemotingCommand processRequest(ChannelHandlerContext ctx, RemotingCommand request)
            throws RemotingCommandException {
        final RemotingCommand response = RemotingCommand.createResponseCommand(null);

        final EndTransactionRequestHeader requestHeader = (EndTransactionRequestHeader) request
            .decodeCommandCustomHeader(EndTransactionRequestHeader.class);

        // 回查应答
        if (requestHeader.getFromTransactionCheck()) {
            switch (requestHeader.getCommitOrRollback()) {
            // 不提交也不回滚
            case MessageSysFlag.TransactionNotType: {
                logTransaction.warn(
                    "check producer[{}] transaction state, but it's pending status.\n"//
                            + "RequestHeader: {} Remark: {}", //
                    RemotingHelper.parseChannelRemoteAddr(ctx.channel()), //
                    requestHeader.toString(), //
                    request.getRemark());
                return null;
            }
                // 提交
            case MessageSysFlag.TransactionCommitType: {
                logTransaction.warn(
                    "check producer[{}] transaction state, the producer commit the message.\n"//
                            + "RequestHeader: {} Remark: {}", //
                    RemotingHelper.parseChannelRemoteAddr(ctx.channel()), //
                    requestHeader.toString(), //
                    request.getRemark());

                break;
            }
                // 回滚
            case MessageSysFlag.TransactionRollbackType: {
                logTransaction.warn(
                    "check producer[{}] transaction state, the producer rollback the message.\n"//
                            + "RequestHeader: {} Remark: {}", //
                    RemotingHelper.parseChannelRemoteAddr(ctx.channel()), //
                    requestHeader.toString(), //
                    request.getRemark());
                break;
            }
            default:
                return null;
            }
        }
        else { // 正常提交回滚
            switch (requestHeader.getCommitOrRollback()) {
            // 不提交也不回滚
            case MessageSysFlag.TransactionNotType: {
                logTransaction.warn(
                    "the producer[{}] end transaction in sending message,  and it's pending status.\n"//
                            + "RequestHeader: {} Remark: {}", //
                    RemotingHelper.parseChannelRemoteAddr(ctx.channel()), //
                    requestHeader.toString(), //
                    request.getRemark());
                return null;
            }
                // 提交
            case MessageSysFlag.TransactionCommitType: {
                break;
            }
                // 回滚
            case MessageSysFlag.TransactionRollbackType: {
                logTransaction.warn(
                    "the producer[{}] end transaction in sending message, rollback the message.\n"//
                            + "RequestHeader: {} Remark: {}", //
                    RemotingHelper.parseChannelRemoteAddr(ctx.channel()), //
                    requestHeader.toString(), //
                    request.getRemark());
                break;
            }
            default:
                return null;
            }
        }

        final MessageExt msgExt = this.brokerController.getMessageStore()
            .lookMessageByOffset(requestHeader.getCommitLogOffset());

        if (msgExt != null) {
            // 检验Producet group
            final String pgroupRead = msgExt.getProperty(MessageConst.PROPERTY_PRODUCER_GROUP);
            if (!pgroupRead.equals(requestHeader.getProducerGroup())) {
                response.setCode(ResponseCode.SYSTEM_ERROR);
                response.setRemark("the producer group wrong");
                return response;
            }

            // 检验Transaction State Table Offset
            if (msgExt.getQueueOffset() != requestHeader.getTranStateTableOffset()) {
                response.setCode(ResponseCode.SYSTEM_ERROR);
                response.setRemark("the transaction state table offset wrong");
                return response;
            }

            // 校验Commit Log Offset
            if (msgExt.getCommitLogOffset() != requestHeader.getCommitLogOffset()) {
                response.setCode(ResponseCode.SYSTEM_ERROR);
                response.setRemark("the commit log offset wrong");
                return response;
            }

            MessageExtBrokerInner msgInner = this.endMessageTransaction(msgExt);
            msgInner.setSysFlag(MessageSysFlag.resetTransactionValue(msgInner.getSysFlag(),
                requestHeader.getCommitOrRollback()));
            msgInner.setQueueOffset(requestHeader.getTranStateTableOffset());
            msgInner.setPreparedTransactionOffset(requestHeader.getCommitLogOffset());
            msgInner.setStoreTimestamp(msgExt.getStoreTimestamp());
            if (MessageSysFlag.TransactionRollbackType == requestHeader.getCommitOrRollback()) {
                msgInner.setBody(null);
            }

            final MessageStore messageStore = this.brokerController.getMessageStore();
            final PutMessageResult putMessageResult = messageStore.putMessage(msgInner);

            if (putMessageResult != null) {
                switch (putMessageResult.getPutMessageStatus()) {
                case PUT_OK:
                case FLUSH_DISK_TIMEOUT:
                case FLUSH_SLAVE_TIMEOUT:
                case SLAVE_NOT_AVAILABLE:
                    response.setCode(ResponseCode.SUCCESS);
                    response.setRemark(null);
                    break;
                // Failed
                case CREATE_MAPEDFILE_FAILED:
                    response.setCode(ResponseCode.SYSTEM_ERROR);
                    response.setRemark("create maped file failed.");
                    break;
                case MESSAGE_ILLEGAL:
                    response.setCode(ResponseCode.MESSAGE_ILLEGAL);
                    response.setRemark("the message is illegal, maybe length not matched.");
                    break;
                case SERVICE_NOT_AVAILABLE:
                    response.setCode(ResponseCode.SERVICE_NOT_AVAILABLE);
                    response.setRemark("service not available now.");
                    break;
                case UNKNOWN_ERROR:
                    response.setCode(ResponseCode.SYSTEM_ERROR);
                    response.setRemark("UNKNOWN_ERROR");
                    break;
                default:
                    response.setCode(ResponseCode.SYSTEM_ERROR);
                    response.setRemark("UNKNOWN_ERROR DEFAULT");
                    break;
                }

                return response;
            }
            else {
                response.setCode(ResponseCode.SYSTEM_ERROR);
                response.setRemark("store putMessage return null");
                return response;
            }
        }
        else {
            response.setCode(ResponseCode.SYSTEM_ERROR);
            response.setRemark("find prepared transaction message failed");
            return response;
        }

    }


    /**
     * 构造message inner
     * 
     * @param msgExt
     * @return
     */
    private MessageExtBrokerInner endMessageTransaction(MessageExt msgExt) {

        MessageExtBrokerInner msgInner = new MessageExtBrokerInner();
        msgInner.setBody(msgExt.getBody());
        msgInner.setFlag(msgExt.getFlag());
        MessageAccessor.setProperties(msgInner, msgExt.getProperties());

        TopicFilterType topicFilterType =
                (msgInner.getSysFlag() & MessageSysFlag.MultiTagsFlag) == MessageSysFlag.MultiTagsFlag
                        ? TopicFilterType.MULTI_TAG : TopicFilterType.SINGLE_TAG;
        long tagsCodeValue = MessageExtBrokerInner.tagsString2tagsCode(topicFilterType, msgInner.getTags());
        msgInner.setTagsCode(tagsCodeValue);
        msgInner.setPropertiesString(MessageDecoder.messageProperties2String(msgExt.getProperties()));

        msgInner.setSysFlag(msgExt.getSysFlag());
        msgInner.setBornTimestamp(msgExt.getBornTimestamp());
        msgInner.setBornHost(msgExt.getBornHost());
        msgInner.setStoreHost(msgExt.getStoreHost());
        msgInner.setReconsumeTimes(msgExt.getReconsumeTimes());

        msgInner.setWaitStoreMsgOK(false);
        MessageAccessor.clearProperty(msgInner, MessageConst.PROPERTY_DELAY_TIME_LEVEL);

        msgInner.setTopic(msgExt.getTopic());
        msgInner.setQueueId(msgExt.getQueueId());

        return msgInner;
    }

}

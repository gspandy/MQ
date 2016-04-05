package com.ztesoft.zsmart.zmq.broker.processor;

import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.util.List;
import java.util.Random;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.netty.channel.ChannelHandlerContext;

import com.ztesoft.zsmart.zmq.broker.BrokerController;
import com.ztesoft.zsmart.zmq.broker.mqtrace.SendMessageContext;
import com.ztesoft.zsmart.zmq.broker.mqtrace.SendMessageHook;
import com.ztesoft.zsmart.zmq.common.MixAll;
import com.ztesoft.zsmart.zmq.common.TopicConfig;
import com.ztesoft.zsmart.zmq.common.TopicFilterType;
import com.ztesoft.zsmart.zmq.common.constant.DBMsgConstants;
import com.ztesoft.zsmart.zmq.common.constant.LoggerName;
import com.ztesoft.zsmart.zmq.common.constant.PermName;
import com.ztesoft.zsmart.zmq.common.help.FAQUrl;
import com.ztesoft.zsmart.zmq.common.message.MessageAccessor;
import com.ztesoft.zsmart.zmq.common.message.MessageDecoder;
import com.ztesoft.zsmart.zmq.common.protocol.RequestCode;
import com.ztesoft.zsmart.zmq.common.protocol.ResponseCode;
import com.ztesoft.zsmart.zmq.common.protocol.header.SendMessageRequestHeader;
import com.ztesoft.zsmart.zmq.common.protocol.header.SendMessageRequestHeaderV2;
import com.ztesoft.zsmart.zmq.common.protocol.header.SendMessageResponseHeader;
import com.ztesoft.zsmart.zmq.common.sysflag.MessageSysFlag;
import com.ztesoft.zsmart.zmq.common.sysflag.TopicSysFlag;
import com.ztesoft.zsmart.zmq.common.utils.ChannelUtil;
import com.ztesoft.zsmart.zmq.remoting.common.RemotingHelper;
import com.ztesoft.zsmart.zmq.remoting.exception.RemotingCommandException;
import com.ztesoft.zsmart.zmq.remoting.netty.NettyRequestProcessor;
import com.ztesoft.zsmart.zmq.remoting.protocol.RemotingCommand;
import com.ztesoft.zsmart.zmq.store.MessageExtBrokerInner;

public abstract class AbstractSendMessageProcessor implements NettyRequestProcessor {
    protected static final Logger log = LoggerFactory.getLogger(LoggerName.BrokerLoggerName);

    protected final static int DLQ_NUMS_PER_GROUP = 1;

    protected final BrokerController brokerController;

    protected final Random random = new Random(System.currentTimeMillis());

    protected final SocketAddress storeHost;

    public AbstractSendMessageProcessor(final BrokerController brokerController) {
        this.brokerController = brokerController;
        this.storeHost = new InetSocketAddress(brokerController.getBrokerConfig().getBrokerIP1(), brokerController
            .getNettyServerConfig().getListenPort());
    }

    private List<SendMessageHook> sendMessageHookList;

    public boolean hasSendMessageHook() {
        return sendMessageHookList != null && !this.sendMessageHookList.isEmpty();
    }

    protected SendMessageContext buildMsgContext(ChannelHandlerContext ctx, SendMessageRequestHeader requestHeader) {
        if (!this.hasSendMessageHook()) {
            return null;
        }

        SendMessageContext mqtraceContext = new SendMessageContext();
        mqtraceContext.setProducerGroup(requestHeader.getProducerGroup());
        mqtraceContext.setTopic(requestHeader.getTopic());
        mqtraceContext.setMsgProps(requestHeader.getProperties());
        mqtraceContext.setBornHost(RemotingHelper.parseChannelRemoteAddr(ctx.channel()));
        ;
        mqtraceContext.setBrokerAddr(this.brokerController.getBrokerAddr());
        return mqtraceContext;
    }

    protected SendMessageRequestHeader parseRequestHeader(RemotingCommand request) throws RemotingCommandException {

        SendMessageRequestHeaderV2 requestHeaderV2 = null;
        SendMessageRequestHeader requestHeader = null;
        switch (request.getCode()) {
            case RequestCode.SEND_MESSAGE_V2:
                requestHeaderV2 = (SendMessageRequestHeaderV2) request
                    .decodeCommandCustomHeader(SendMessageRequestHeaderV2.class);
            case RequestCode.SEND_MESSAGE:
                if (null == requestHeaderV2) {
                    requestHeader = (SendMessageRequestHeader) request
                        .decodeCommandCustomHeader(SendMessageRequestHeader.class);
                }
                else {
                    requestHeader = SendMessageRequestHeaderV2.createSendMessageRequestHeaderV1(requestHeaderV2);
                }
            default:
                break;
        }
        return requestHeader;
    }

    protected MessageExtBrokerInner buildInnerMsge(//
        final ChannelHandlerContext ctx,//
        final SendMessageRequestHeader requestHeader,//
        final byte[] body,//
        TopicConfig topicConfig) {

        int queueIdInt = requestHeader.getQueueId();

        if (queueIdInt < 0) {
            queueIdInt = Math.abs(this.random.nextInt() % 99999999) % topicConfig.getWriteQueueNums();
        }

        int sysFlag = requestHeader.getSysFlag();

        //
        if (TopicFilterType.MULTI_TAG == topicConfig.getTopicFilterType()) {
            sysFlag |= MessageSysFlag.MultiTagsFlag;
        }

        MessageExtBrokerInner msgInner = new MessageExtBrokerInner();
        msgInner.setTopic(requestHeader.getTopic());
        msgInner.setBody(body);
        msgInner.setFlag(requestHeader.getFlag());
        MessageAccessor.setProperties(msgInner, MessageDecoder.string2messageProperties(requestHeader.getProperties()));
        msgInner.setPropertiesString(requestHeader.getProperties());
        msgInner.setTagsCode(MessageExtBrokerInner.tagsString2tagsCode(topicConfig.getTopicFilterType(),
            msgInner.getTags()));
        msgInner.setQueueId(queueIdInt);
        msgInner.setSysFlag(sysFlag);
        msgInner.setBornTimestamp(requestHeader.getBornTimestamp());
        msgInner.setReconsumeTimes(requestHeader.getReconsumeTimes() == null ? 0 : requestHeader.getReconsumeTimes());
        return msgInner;
    }

    protected RemotingCommand msgContentCheck(final ChannelHandlerContext ctx,//
        final SendMessageRequestHeader requestHeader,//
        final RemotingCommand request,//
        final RemotingCommand response) {
        // nessage topic 长度检验
        if (requestHeader.getTopic().length() > Byte.MAX_VALUE) {
            log.warn("putMessage message topic length too long " + requestHeader.getTopic().length());
            response.setCode(ResponseCode.MESSAGE_ILLEGAL);
            return response;
        }

        // message properties长度校验
        if (requestHeader.getProperties() != null && requestHeader.getProperties().length() > Short.MAX_VALUE) {
            log.warn("putMessage message properties length too long " + requestHeader.getProperties().length());
            response.setCode(ResponseCode.MESSAGE_ILLEGAL);
            return response;
        }

        if (request.getBody().length > DBMsgConstants.maxBodySize) {
            log.warn(" topic {}  msg body size {}  from {}", requestHeader.getTopic(), request.getBody().length,
                ChannelUtil.getRemoteIp(ctx.channel()));
            response.setRemark("msg body must be less 64KB");
            response.setCode(ResponseCode.MESSAGE_ILLEGAL);
            return response;
        }
        return response;
    }

    protected RemotingCommand msgCheck(final ChannelHandlerContext ctx,//
        SendMessageRequestHeader requestHeader,//
        final RemotingCommand response) {

        if (!PermName.isWriteable(this.brokerController.getBrokerConfig().getBrokerPermission())
            && this.brokerController.getTopicConfigManager().isOrderTopic(requestHeader.getTopic())) {
            response.setCode(ResponseCode.NO_PERMISSION);
            response.setRemark("the broker[" + this.brokerController.getBrokerConfig().getBrokerIP1()
                + "] sending message is forbidden");
            return response;
        }

        if (!this.brokerController.getTopicConfigManager().isTopicCanSendMessage(requestHeader.getTopic())) {
            String errorMsg = "the topic[" + requestHeader.getTopic() + "] is conflict with system reserved words.";
            log.warn(errorMsg);
            response.setCode(ResponseCode.SYSTEM_ERROR);
            response.setRemark(errorMsg);
            return response;
        }

        TopicConfig topicConfig = this.brokerController.getTopicConfigManager().selectTopicConfig(
            requestHeader.getTopic());

        if (topicConfig == null) {
            int topicSysFlag = 0;
            if (requestHeader.isUnitMode()) {
                if (requestHeader.getTopic().startsWith(MixAll.RETRY_GROUP_TOPIC_PREFIX)) {
                    topicSysFlag = TopicSysFlag.buildSysFlag(false, true);
                }
                else {
                    topicSysFlag = TopicSysFlag.buildSysFlag(true, false);
                }
            }

            log.warn("the topic " + requestHeader.getTopic() + " not exist, producer: " + ctx.channel().remoteAddress());

            topicConfig = this.brokerController.getTopicConfigManager().createTopicInSendMessageMethod(//
                requestHeader.getTopic(), //
                requestHeader.getDefaultTopic(), //
                RemotingHelper.parseChannelRemoteAddr(ctx.channel()), //
                requestHeader.getDefaultTopicQueueNums(), topicSysFlag);

            // 灏濊瘯鐪嬩笅鏄惁鏄け璐ユ秷鎭彂鍥�
            if (null == topicConfig) {
                if (requestHeader.getTopic().startsWith(MixAll.RETRY_GROUP_TOPIC_PREFIX)) {
                    topicConfig = this.brokerController.getTopicConfigManager().createTopicInSendMessageBackMethod(
                        requestHeader.getTopic(), 1, PermName.PERM_WRITE | PermName.PERM_READ, topicSysFlag);
                }
            }

            if (null == topicConfig) {
                response.setCode(ResponseCode.TOPIC_NOT_EXIST);
                response.setRemark("topic[" + requestHeader.getTopic() + "] not exist, apply first please!"
                    + FAQUrl.suggestTodo(FAQUrl.APPLY_TOPIC_URL));
                return response;
            }
        }

        int queueIdInt = requestHeader.getQueueId();
        int idValid = Math.max(topicConfig.getWriteQueueNums(), topicConfig.getReadQueueNums());

        if (queueIdInt > idValid) {
            String errorInfo = String.format("request queueId[%d] is illagal, %s Producer: %s",//
                queueIdInt,//
                topicConfig.toString(),//
                RemotingHelper.parseChannelRemoteAddr(ctx.channel()));

            log.warn(errorInfo);
            response.setCode(ResponseCode.SYSTEM_ERROR);
            response.setRemark(errorInfo);

            return response;
        }

        return response;

    }

    public SocketAddress getStoreHost() {
        return storeHost;
    }

    protected void doResponse(ChannelHandlerContext ctx, RemotingCommand request, final RemotingCommand response) {
        if (!request.isOnewayRPC()) {
            try {
                ctx.writeAndFlush(response);
            }
            catch (Throwable e) {
                log.error("SendMessageProcessor process request over, but response failed", e);
                log.error(request.toString());
                log.error(response.toString());
            }
        }
    }

    public void executeSendMessageHookBefore(final ChannelHandlerContext ctx, final RemotingCommand request,
        SendMessageContext context) {
        if (hasSendMessageHook()) {
            for (SendMessageHook hook : this.sendMessageHookList) {
                try {
                    final SendMessageRequestHeader requestHeader = (SendMessageRequestHeader) request
                        .decodeCommandCustomHeader(SendMessageRequestHeader.class);
                    context.setProducerGroup(requestHeader.getProducerGroup());
                    context.setTopic(requestHeader.getTopic());
                    context.setBodyLength(request.getBody().length);
                    context.setMsgProps(requestHeader.getProperties());
                    context.setBornHost(RemotingHelper.parseChannelRemoteAddr(ctx.channel()));
                    context.setBrokerAddr(this.brokerController.getBrokerAddr());
                    context.setQueueId(requestHeader.getQueueId());
                    hook.sendMessageBefore(context);
                    requestHeader.setProperties(context.getMsgProps());
                }
                catch (Throwable e) {
                }
            }
        }

    }
    
    public void executeSendMessageHookAfter(final RemotingCommand response, final SendMessageContext context) {
        if (hasSendMessageHook()) {
            for (SendMessageHook hook : this.sendMessageHookList) {
                try {
                    if (response != null) {
                        final SendMessageResponseHeader responseHeader =
                                (SendMessageResponseHeader) response.readCustomHeader();
                        context.setMsgId(responseHeader.getMsgId());
                        context.setQueueId(responseHeader.getQueueId());
                        context.setQueueOffset(responseHeader.getQueueOffset());
                        context.setCode(response.getCode());
                        context.setErrorMsg(response.getRemark());
                    }
                    hook.sendMessageAfter(context);
                }
                catch (Throwable e) {
                    
                }
            }
        }
    }

}

package com.ztesoft.zsmart.zmq.broker.processor;

import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.ztesoft.zsmart.zmq.broker.BrokerController;
import com.ztesoft.zsmart.zmq.broker.client.ClientChannelInfo;
import com.ztesoft.zsmart.zmq.broker.client.ConsumerGroupInfo;
import com.ztesoft.zsmart.zmq.broker.mqtrace.ConsumeMessageContext;
import com.ztesoft.zsmart.zmq.broker.mqtrace.ConsumeMessageHook;
import com.ztesoft.zsmart.zmq.client.consumer.listener.ConsumeConcurrentlyStatus;
import com.ztesoft.zsmart.zmq.common.MixAll;
import com.ztesoft.zsmart.zmq.common.constant.LoggerName;
import com.ztesoft.zsmart.zmq.common.constant.PermName;
import com.ztesoft.zsmart.zmq.common.protocol.RequestCode;
import com.ztesoft.zsmart.zmq.common.protocol.ResponseCode;
import com.ztesoft.zsmart.zmq.common.protocol.header.GetConsumerListByGroupRequestHeader;
import com.ztesoft.zsmart.zmq.common.protocol.header.GetConsumerListByGroupResponseBody;
import com.ztesoft.zsmart.zmq.common.protocol.header.QueryConsumerOffsetRequestHeader;
import com.ztesoft.zsmart.zmq.common.protocol.header.QueryConsumerOffsetResponseHeader;
import com.ztesoft.zsmart.zmq.common.protocol.header.UnregisterClientRequestHeader;
import com.ztesoft.zsmart.zmq.common.protocol.header.UpdateConsumerOffsetRequestHeader;
import com.ztesoft.zsmart.zmq.common.protocol.header.UpdateConsumerOffsetResponseHeader;
import com.ztesoft.zsmart.zmq.common.protocol.heartbeat.ConsumerData;
import com.ztesoft.zsmart.zmq.common.protocol.heartbeat.HeartbeatData;
import com.ztesoft.zsmart.zmq.common.protocol.heartbeat.ProducerData;
import com.ztesoft.zsmart.zmq.common.subscription.SubscriptionGroupConfig;
import com.ztesoft.zsmart.zmq.common.sysflag.TopicSysFlag;
import com.ztesoft.zsmart.zmq.remoting.common.RemotingHelper;
import com.ztesoft.zsmart.zmq.remoting.exception.RemotingCommandException;
import com.ztesoft.zsmart.zmq.remoting.netty.NettyRequestProcessor;
import com.ztesoft.zsmart.zmq.remoting.protocol.RemotingCommand;

import io.netty.channel.ChannelHandlerContext;


/**
 * Client注册与注销管理
 * 
 * @author J.Wang
 *
 */
public class ClientManageProcessor implements NettyRequestProcessor {
    private static final Logger log = LoggerFactory.getLogger(LoggerName.BrokerLoggerName);

    private final BrokerController brokerController;
    /**
     * 消费每条消息会回调
     */
    private List<ConsumeMessageHook> consumeMessageHookList;


    public boolean hasConsumeMessageHook() {
        return consumeMessageHookList != null && !this.consumeMessageHookList.isEmpty();
    }


    public void registerConsumeMessageHook(List<ConsumeMessageHook> consumeMessageHookList) {
        this.consumeMessageHookList = consumeMessageHookList;
    }


    public ClientManageProcessor(final BrokerController brokerController) {
        this.brokerController = brokerController;
    }


    public void executeConsumeMessageHookAfter(final ConsumeMessageContext context) {
        if (hasConsumeMessageHook()) {
            for (ConsumeMessageHook hook : this.consumeMessageHookList) {
                try {
                    hook.consumeMessageAfter(context);
                }
                catch (Throwable e) {
                }
            }
        }
    }


    @Override
    public RemotingCommand processRequest(ChannelHandlerContext ctx, RemotingCommand request)
            throws RemotingCommandException {
        switch (request.getCode()) {
        case RequestCode.HEART_BEAT: // 心跳
            return this.heartBeat(ctx, request);
        case RequestCode.UNREGISTER_CLIENT: // 注销client
            return this.unregisterClient(ctx, request);
        case RequestCode.GET_CONSUMER_LIST_BY_GROUP:
            return this.getConsumerListByGroup(ctx, request); // Broker
                                                              // 获取ConsumerId列表通过GroupName
        case RequestCode.UPDATE_CONSUMER_OFFSET:// Broker 更新Consumer Offset
            return this.updateConsumerOffset(ctx, request);
        case RequestCode.QUERY_CONSUMER_OFFSET:
            return this.queryConsumerOffset(ctx, request);
        default:
            break;
        }
        return null;
    }


    /**
     * 查询消费offset
     * 
     * @param ctx
     * @param request
     * @return
     * @throws RemotingCommandException
     */
    private RemotingCommand queryConsumerOffset(ChannelHandlerContext ctx, RemotingCommand request)
            throws RemotingCommandException {
        final RemotingCommand response =
                RemotingCommand.createResponseCommand(QueryConsumerOffsetResponseHeader.class);
        final QueryConsumerOffsetResponseHeader responseHeader =
                (QueryConsumerOffsetResponseHeader) response.readCustomHeader();
        final QueryConsumerOffsetRequestHeader requestHeader = (QueryConsumerOffsetRequestHeader) request
            .decodeCommandCustomHeader(QueryConsumerOffsetRequestHeader.class);

        long offset = this.brokerController.getConsumerOffsetManager().queryOffset(
            requestHeader.getConsumerGroup(), requestHeader.getTopic(), requestHeader.getQueueId());
        // 订阅组存在
        if (offset >= 0) {
            responseHeader.setOffset(offset);
            response.setCode(ResponseCode.SUCCESS);
            response.setRemark(null);
        }
        // 订阅组不存在
        else {
            long minOffset = this.brokerController.getMessageStore()
                .getMinOffsetInQuque(requestHeader.getTopic(), requestHeader.getQueueId());
            // 订阅组不存在情况下，如果这个队列的消息最小Offset是0，则表示这个Topic上线时间不长，服务器堆积的数据也不多，那么这个订阅组就从0开始消费。
            // 尤其对于Topic队列数动态扩容时，必须要从0开始消费。
            if (minOffset <= 0 && !this.brokerController.getMessageStore()
                .checkInDiskByConsumeOffset(requestHeader.getTopic(), requestHeader.getQueueId(), 0)) {
                responseHeader.setOffset(0L);
                response.setCode(ResponseCode.SUCCESS);
                response.setRemark(null);
            }
            // 新版本服务器不做消费进度纠正
            else {
                response.setCode(ResponseCode.QUERY_NOT_FOUND);
                response.setRemark("Not found, V3_0_6_SNAPSHOT maybe this group consumer boot first");
            }
        }

        return response;
    }


    /**
     * 更新Consumer Offset
     * 
     * @param ctx
     * @param request
     * @return
     * @throws RemotingCommandException
     */
    private RemotingCommand updateConsumerOffset(ChannelHandlerContext ctx, RemotingCommand request)
            throws RemotingCommandException {
        final RemotingCommand response =
                RemotingCommand.createResponseCommand(UpdateConsumerOffsetResponseHeader.class);
        final UpdateConsumerOffsetRequestHeader requestHeader = (UpdateConsumerOffsetRequestHeader) request
            .decodeCommandCustomHeader(UpdateConsumerOffsetRequestHeader.class);

        // 消息轨迹：记录已经消费成功并提交 offset 的消息记录
        if (this.hasConsumeMessageHook()) {
            // 执行hook
            ConsumeMessageContext context = new ConsumeMessageContext();
            context.setConsumerGroup(requestHeader.getConsumerGroup());
            context.setTopic(requestHeader.getTopic());
            context.setClientHost(RemotingHelper.parseChannelRemoteAddr(ctx.channel()));
            context.setSuccess(true);
            context.setStatus(ConsumeConcurrentlyStatus.CONSUME_SUCCESS.toString());

            final SocketAddress storeHost =
                    new InetSocketAddress(brokerController.getBrokerConfig().getBrokerIP1(),
                        brokerController.getNettyServerConfig().getListenPort());

            long preOffset = this.brokerController.getConsumerOffsetManager().queryOffset(
                requestHeader.getConsumerGroup(), requestHeader.getTopic(), requestHeader.getQueueId());
            Map<String, Long> messageIds =
                    this.brokerController.getMessageStore().getMessageIds(requestHeader.getTopic(),
                        requestHeader.getQueueId(), preOffset, requestHeader.getCommitOffset(), storeHost);
            context.setMessageIds(messageIds);

            this.executeConsumeMessageHookAfter(context);
        }
        this.brokerController.getConsumerOffsetManager().commitOffset(requestHeader.getConsumerGroup(),
            requestHeader.getTopic(), requestHeader.getQueueId(), requestHeader.getCommitOffset());
        response.setCode(ResponseCode.SUCCESS);
        response.setRemark(null);
        return response;
    }


    /**
     * // Broker 获取ConsumerId列表通过GroupName
     * 
     * @param ctx
     * @param request
     * @return
     * @throws RemotingCommandException
     */
    private RemotingCommand getConsumerListByGroup(ChannelHandlerContext ctx, RemotingCommand request)
            throws RemotingCommandException {
        final RemotingCommand response = RemotingCommand.createResponseCommand(null);
        final GetConsumerListByGroupRequestHeader requestHeader =
                (GetConsumerListByGroupRequestHeader) request
                    .decodeCommandCustomHeader(GetConsumerListByGroupRequestHeader.class);

        ConsumerGroupInfo consumerGroupInfo = this.brokerController.getConsumerManager()
            .getConsumerGroupInfo(requestHeader.getConsumerGroup());
        if (consumerGroupInfo != null) {
            List<String> clientIds = consumerGroupInfo.getAllClientId();
            if (!clientIds.isEmpty()) {
                GetConsumerListByGroupResponseBody body = new GetConsumerListByGroupResponseBody();
                body.setConsumerIdList(clientIds);
                response.setBody(body.encode());
                response.setCode(ResponseCode.SUCCESS);
                response.setRemark(null);
                return response;
            }
            else {
                log.warn("getAllClientId failed, {} {}", requestHeader.getConsumerGroup(),
                    RemotingHelper.parseChannelRemoteAddr(ctx.channel()));
            }
        }
        else {
            log.warn("getConsumerGroupInfo failed, {} {}", requestHeader.getConsumerGroup(),
                RemotingHelper.parseChannelRemoteAddr(ctx.channel()));
        }

        response.setCode(ResponseCode.SYSTEM_ERROR);
        response.setRemark("no consumer for this group, " + requestHeader.getConsumerGroup());
        return response;
    }


    /**
     * 注销client
     * 
     * @param ctx
     * @param request
     * @return
     * @throws RemotingCommandException
     */
    private RemotingCommand unregisterClient(ChannelHandlerContext ctx, RemotingCommand request)
            throws RemotingCommandException {
        final RemotingCommand response = RemotingCommand.createResponseCommand(null);
        final UnregisterClientRequestHeader requestHeader = (UnregisterClientRequestHeader) request
            .decodeCommandCustomHeader(UnregisterClientRequestHeader.class);

        ClientChannelInfo clientChannelInfo = new ClientChannelInfo(//
            ctx.channel(), //
            requestHeader.getClientID(), //
            request.getLanguage(), //
            request.getVersion()//
        );

        // 注销Producer
        {
            final String group = requestHeader.getProducerGroup();
            if (group != null) {
                this.brokerController.getProducerManager().unregisterProducer(group, clientChannelInfo);
            }
        }

        // 注销Consumer
        {
            final String group = requestHeader.getConsumerGroup();
            if (group != null) {
                this.brokerController.getConsumerManager().unregisterConsumer(group, clientChannelInfo);
            }
        }

        response.setCode(ResponseCode.SUCCESS);
        response.setRemark(null);
        return response;
    }


    /**
     * 处理producer心跳
     * 
     * @param ctx
     * @param request
     * @return
     */
    private RemotingCommand heartBeat(ChannelHandlerContext ctx, RemotingCommand request) {
        RemotingCommand response = RemotingCommand.createResponseCommand(null);

        HeartbeatData heartbeatData = HeartbeatData.decode(request.getBody(), HeartbeatData.class);

        ClientChannelInfo channelInfo = new ClientChannelInfo(ctx.channel(), heartbeatData.getClientID(),
            request.getLanguage(), request.getVersion());

        // 注册consumer
        for (ConsumerData data : heartbeatData.getConsumerDataSet()) {
            SubscriptionGroupConfig subscriptionGroupConfig = this.brokerController
                .getSubscriptionGroupManager().findSubscriptionGroupConfig(data.getGroupName());

            if (subscriptionGroupConfig != null) {
                // 如果是单元化模式 则对topic 进行设置
                int topicSysFlag = 0;
                if (data.isUnitMode()) {
                    topicSysFlag = TopicSysFlag.buildSysFlag(false, true);
                }

                String newTopic = MixAll.getRetryTopic(data.getGroupName());
                this.brokerController.getTopicConfigManager().createTopicInSendMessageBackMethod(//
                    newTopic, //
                    subscriptionGroupConfig.getRetryQueueNums(), //
                    PermName.PERM_WRITE | PermName.PERM_READ, topicSysFlag);
            }

            boolean changed = this.brokerController.getConsumerManager().registerConsumer(data.getGroupName(), //
                channelInfo, //
                data.getConsumeType(), //
                data.getMessageModel(), //
                data.getConsumeFromWhere(), //
                data.getSubscriptionDataSet()//
            );

            if (changed) {
                log.info("registerConsumer info changed {} {}", //
                    data.toString(), //
                    RemotingHelper.parseChannelRemoteAddr(ctx.channel())//
                );
            }
        }

        // 注册Producer
        for (ProducerData data : heartbeatData.getProducerDataSet()) {
            this.brokerController.getProducerManager().registerProducer(data.getGroupName(), channelInfo);
        }

        response.setCode(ResponseCode.SUCCESS);
        response.setRemark(null);
        return response;
    }

}

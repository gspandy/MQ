package com.ztesoft.zsmart.zmq.broker.client.net;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.ztesoft.zsmart.zmq.broker.BrokerController;
import com.ztesoft.zsmart.zmq.broker.client.ClientChannelInfo;
import com.ztesoft.zsmart.zmq.broker.client.ConsumerGroupInfo;
import com.ztesoft.zsmart.zmq.broker.pagecache.OneMessageTransfer;
import com.ztesoft.zsmart.zmq.common.MQVersion;
import com.ztesoft.zsmart.zmq.common.TopicConfig;
import com.ztesoft.zsmart.zmq.common.UtilAll;
import com.ztesoft.zsmart.zmq.common.constant.LoggerName;
import com.ztesoft.zsmart.zmq.common.message.MessageQueue;
import com.ztesoft.zsmart.zmq.common.protocol.RequestCode;
import com.ztesoft.zsmart.zmq.common.protocol.ResponseCode;
import com.ztesoft.zsmart.zmq.common.protocol.body.GetConsumerStatusBody;
import com.ztesoft.zsmart.zmq.common.protocol.body.ResetOffsetBody;
import com.ztesoft.zsmart.zmq.common.protocol.header.GetConsumerStatusRequestHeader;
import com.ztesoft.zsmart.zmq.common.protocol.header.NotifyConsumerIdsChangedRequestHeader;
import com.ztesoft.zsmart.zmq.common.protocol.header.ResetOffsetRequestHeader;
import com.ztesoft.zsmart.zmq.common.protocol.header.namesrv.CheckTransactionStateRequestHeader;
import com.ztesoft.zsmart.zmq.remoting.common.RemotingHelper;
import com.ztesoft.zsmart.zmq.remoting.exception.RemotingSendRequestException;
import com.ztesoft.zsmart.zmq.remoting.exception.RemotingTimeoutException;
import com.ztesoft.zsmart.zmq.remoting.protocol.RemotingCommand;
import com.ztesoft.zsmart.zmq.store.SelectMapedBufferResult;

import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.FileRegion;


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
     *         <br>
     */
    public void checkProducerTransactionState(//
            final Channel channel, //
            final CheckTransactionStateRequestHeader requestHeader, //
            final SelectMapedBufferResult selectMapedBufferResult//
    ) {
        RemotingCommand request =
                RemotingCommand.createRequestCommand(RequestCode.CHECK_TRANSACTION_STATE, requestHeader);

        request.markOnewayRPC();
        try {
            FileRegion fileRegion = new OneMessageTransfer(
                request.encodeHeader(selectMapedBufferResult.getSize()), selectMapedBufferResult);

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
     * @throws InterruptedException
     *             <br>
     */
    public RemotingCommand callClient(//
            final Channel channel, //
            final RemotingCommand request)
                    throws RemotingSendRequestException, RemotingTimeoutException, InterruptedException {
        return this.brokerController.getRemotingServer().invokeSync(channel, request, 10000);
    }


    /**
     * Broker主动通知Consumer id列表发生变化: <br>
     * 
     * @author wang.jun<br>
     * @taskId <br>
     * @param channel
     * @param consumerGroup
     *            <br>
     */
    public void notifyConsumerIdsChanged(//
            final Channel channel, //
            final String consumerGroup) {
        if (consumerGroup == null) {
            log.error("notifyConsumerIdsChanged consumerGroup is null");
            return;
        }

        NotifyConsumerIdsChangedRequestHeader requestHeader = new NotifyConsumerIdsChangedRequestHeader();

        requestHeader.setConsumerGroup(consumerGroup);

        RemotingCommand request =
                RemotingCommand.createRequestCommand(RequestCode.NOTIFY_CONSUMER_IDS_CHANGED, requestHeader);

        try {
            this.brokerController.getRemotingServer().invokeOneway(channel, request, 10);
        }
        catch (Exception e) {
            log.error("notifyConsumerIdsChanged exception, " + consumerGroup, e);
        }
    }


    /**
     * Broker 主动通知 Consumer，offset 需要进行重置列表发生变化
     * 
     * @param topic
     * @param group
     * @param timeStamp
     * @param isForce
     * @return
     */
    public RemotingCommand resetOffset(String topic, String group, long timeStamp, boolean isForce) {
        final RemotingCommand response = RemotingCommand.createResponseCommand(null);
        TopicConfig topicConfig = this.brokerController.getTopicConfigManager().selectTopicConfig(topic);
        if (topicConfig == null) {
            log.error("[reset-offset] reset offset failed, no topic in this broker. topic={}", topic);
            response.setCode(ResponseCode.SYSTEM_ERROR);
            response.setRemark("[reset-offset] reset offset failed, no topic in this broker. topic=" + topic);
            return response;
        }

        Map<MessageQueue, Long> offsetTable = new HashMap<MessageQueue, Long>();

        for (int i = 0; i < topicConfig.getWriteQueueNums(); i++) {
            MessageQueue mq = new MessageQueue();
            mq.setBrokerName(this.brokerController.getBrokerConfig().getBrokerName());
            mq.setTopic(topic);
            mq.setQueueId(i);

            long consumerOffset =
                    this.brokerController.getConsumerOffsetManager().queryOffset(group, topic, i);

            if (-1 == consumerOffset) {
                response.setCode(ResponseCode.SYSTEM_ERROR);
                response.setRemark(String.format("THe consumer group <%s> not exist", group));
                return response;
            }

            long timeStampOffset =
                    this.brokerController.getMessageStore().getOffsetInQueueByTime(topic, i, timeStamp);

            if (isForce || timeStampOffset < consumerOffset) {
                offsetTable.put(mq, timeStampOffset);
            }
            else {
                offsetTable.put(mq, consumerOffset);
            }
        }

        ResetOffsetRequestHeader requestHeader = new ResetOffsetRequestHeader();
        requestHeader.setTopic(topic);
        requestHeader.setGroup(group);
        requestHeader.setTimestamp(timeStamp);
        requestHeader.setForce(isForce);

        RemotingCommand request =
                RemotingCommand.createRequestCommand(RequestCode.RESET_CONSUMER_CLIENT_OFFSET, requestHeader);

        ResetOffsetBody body = new ResetOffsetBody();
        body.setOffsetTable(offsetTable);

        request.setBody(body.encode());

        ConsumerGroupInfo consumerGroupInfo =
                this.brokerController.getConsumerManager().getConsumerGroupInfo(group);

        // Consumer在线
        if (consumerGroupInfo != null && !consumerGroupInfo.getAllChannel().isEmpty()) {
            ConcurrentHashMap<Channel, ClientChannelInfo> channelInfoTable =
                    consumerGroupInfo.getChannelInfoTable();

            for (Channel channel : channelInfoTable.keySet()) {
                int version = channelInfoTable.get(channel).getVersion();
                if (version >= MQVersion.Version.V3_0_7_SNAPSHOT.ordinal()) {
                    try {
                        this.brokerController.getRemotingServer().invokeOneway(channel, request, 5000);
                        log.info("[reset-offset] reset offset success. topic={}, group={}, clientId={}",
                            new Object[] { topic, group, channelInfoTable.get(channel).getClientId() });
                    }
                    catch (Exception e) {
                        log.error("[reset-offset] reset offset exception. topic={}, group={}",
                            new Object[] { topic, group }, e);
                    }
                }
                else {
                    // 如果有一个客户端是不支持该功能的，则直接返回错误，需要应用方升级。
                    response.setCode(ResponseCode.SYSTEM_ERROR);
                    response.setRemark("the client does not support this feature. version="
                            + MQVersion.getVersionDesc(version));
                    log.warn("[reset-offset] the client does not support this feature. version={}",
                        RemotingHelper.parseChannelRemoteAddr(channel), MQVersion.getVersionDesc(version));
                    return response;
                }
            }
        }
        else {// Consumer不在线
            String errorInfo = String.format(
                "Consumer not online, so can not reset offset, Group: %s Topic: %s Timestamp: %d", //
                requestHeader.getGroup(), //
                requestHeader.getTopic(), //
                requestHeader.getTimestamp());
            log.error(errorInfo);
            response.setCode(ResponseCode.CONSUMER_NOT_ONLINE);
            response.setRemark(errorInfo);
            return response;
        }

        response.setCode(ResponseCode.SUCCESS);
        ResetOffsetBody resBody = new ResetOffsetBody();
        resBody.setOffsetTable(offsetTable);
        response.setBody(resBody.encode());
        return response;
    }


    /**
     * Broker主动获取Consumer端的消息情况
     * 
     * @param topic
     * @param group
     * @param originClientId
     * @return
     */
    public RemotingCommand getConsumeStatus(String topic, String group, String originClientId) {
        final RemotingCommand result = RemotingCommand.createResponseCommand(null);
        GetConsumerStatusRequestHeader requestHeader = new GetConsumerStatusRequestHeader();
        requestHeader.setTopic(topic);
        requestHeader.setGroup(group);

        RemotingCommand request = RemotingCommand
            .createRequestCommand(RequestCode.GET_CONSUMER_STATUS_FROM_CLIENT, requestHeader);

        Map<String, Map<MessageQueue, Long>> consumerStatusTable =
                new HashMap<String, Map<MessageQueue, Long>>();

        ConcurrentHashMap<Channel, ClientChannelInfo> channelInfoTable =
                this.brokerController.getConsumerManager().getConsumerGroupInfo(group).getChannelInfoTable();
        if (null == channelInfoTable || channelInfoTable.isEmpty()) {
            result.setCode(ResponseCode.SYSTEM_ERROR);
            result.setRemark(String.format("No Any Consumer online in the consumer group: [%s]", group));
            return result;
        }

        for (Channel channel : channelInfoTable.keySet()) {
            int version = channelInfoTable.get(channel).getVersion();
            String clientId = channelInfoTable.get(channel).getClientId();
            if (version < MQVersion.Version.V3_0_7_SNAPSHOT.ordinal()) {
                // 如果有一个客户端是不支持该功能的，则直接返回错误，需要应用方升级。
                result.setCode(ResponseCode.SYSTEM_ERROR);
                result.setRemark(
                    "the client does not support this feature. version=" + MQVersion.getVersionDesc(version));
                log.warn("[get-consumer-status] the client does not support this feature. version={}",
                    RemotingHelper.parseChannelRemoteAddr(channel), MQVersion.getVersionDesc(version));
                return result;
            }
            else if (UtilAll.isBlank(originClientId) || originClientId.equals(clientId)) {
                // 不指定 originClientId 则对所有的 client 进行处理；若指定 originClientId 则只对当前
                // originClientId 进行处理

                try {
                    RemotingCommand response =
                            this.brokerController.getRemotingServer().invokeSync(channel, request, 5000);
                    assert response != null;
                    switch (response.getCode()) {
                    case ResponseCode.SUCCESS: {
                        if (response.getBody() != null) {
                            GetConsumerStatusBody body = GetConsumerStatusBody.decode(response.getBody(),
                                GetConsumerStatusBody.class);

                            consumerStatusTable.put(clientId, body.getMessageQueueTable());
                            log.info(
                                "[get-consumer-status] get consumer status success. topic={}, group={}, channelRemoteAddr={}",
                                new Object[] { topic, group, clientId });
                        }
                    }
                    default:
                        break;
                    }
                }
                catch (Exception e) {
                    log.error(
                        "[get-consumer-status] get consumer status exception. topic={}, group={}, offset={}",
                        new Object[] { topic, group }, e);
                }

                // 若指定 originClientId 相应的 client 处理完成，则退出循环
                if (!UtilAll.isBlank(originClientId) && originClientId.equals(clientId)) {
                    break;
                }
            }
        }
        result.setCode(ResponseCode.SUCCESS);
        GetConsumerStatusBody resBody = new GetConsumerStatusBody();
        resBody.setConsumerTable(consumerStatusTable);
        result.setBody(resBody.encode());
        return result;
    }

}

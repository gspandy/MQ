package com.ztesoft.zsmart.zmq.client.impl;

import java.io.UnsupportedEncodingException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import org.slf4j.Logger;

import com.ztesoft.zsmart.zmq.client.VirtualEnvUtil;
import com.ztesoft.zsmart.zmq.client.exception.MQBrokerException;
import com.ztesoft.zsmart.zmq.client.exception.MQClientException;
import com.ztesoft.zsmart.zmq.client.impl.consumer.PullCallback;
import com.ztesoft.zsmart.zmq.client.impl.consumer.PullResult;
import com.ztesoft.zsmart.zmq.client.impl.consumer.PullResultExt;
import com.ztesoft.zsmart.zmq.client.impl.consumer.PullStatus;
import com.ztesoft.zsmart.zmq.client.log.ClientLogger;
import com.ztesoft.zsmart.zmq.client.producer.SendCallback;
import com.ztesoft.zsmart.zmq.client.producer.SendResult;
import com.ztesoft.zsmart.zmq.client.producer.SendStatus;
import com.ztesoft.zsmart.zmq.common.MQVersion;
import com.ztesoft.zsmart.zmq.common.MixAll;
import com.ztesoft.zsmart.zmq.common.TopicConfig;
import com.ztesoft.zsmart.zmq.common.UtilAll;
import com.ztesoft.zsmart.zmq.common.admin.ConsumeStats;
import com.ztesoft.zsmart.zmq.common.admin.OffsetWrapper;
import com.ztesoft.zsmart.zmq.common.admin.TopicOffset;
import com.ztesoft.zsmart.zmq.common.admin.TopicStatsTable;
import com.ztesoft.zsmart.zmq.common.message.Message;
import com.ztesoft.zsmart.zmq.common.message.MessageDecoder;
import com.ztesoft.zsmart.zmq.common.message.MessageExt;
import com.ztesoft.zsmart.zmq.common.message.MessageQueue;
import com.ztesoft.zsmart.zmq.common.namesrv.NamesrvUtil;
import com.ztesoft.zsmart.zmq.common.namesrv.TopAddressing;
import com.ztesoft.zsmart.zmq.common.protocol.RequestCode;
import com.ztesoft.zsmart.zmq.common.protocol.ResponseCode;
import com.ztesoft.zsmart.zmq.common.protocol.body.BrokerStatsData;
import com.ztesoft.zsmart.zmq.common.protocol.body.ClusterInfo;
import com.ztesoft.zsmart.zmq.common.protocol.body.ConsumeMessageDirectlyResult;
import com.ztesoft.zsmart.zmq.common.protocol.body.ConsumerConnection;
import com.ztesoft.zsmart.zmq.common.protocol.body.ConsumerRunningInfo;
import com.ztesoft.zsmart.zmq.common.protocol.body.GetConsumerStatusBody;
import com.ztesoft.zsmart.zmq.common.protocol.body.GroupList;
import com.ztesoft.zsmart.zmq.common.protocol.body.KVTable;
import com.ztesoft.zsmart.zmq.common.protocol.body.LockBatchRequestBody;
import com.ztesoft.zsmart.zmq.common.protocol.body.LockBatchResponseBody;
import com.ztesoft.zsmart.zmq.common.protocol.body.ProducerConnection;
import com.ztesoft.zsmart.zmq.common.protocol.body.QueryConsumeTimeSpanBody;
import com.ztesoft.zsmart.zmq.common.protocol.body.QueryCorrectionOffsetBody;
import com.ztesoft.zsmart.zmq.common.protocol.body.QueueTimeSpan;
import com.ztesoft.zsmart.zmq.common.protocol.body.ResetOffsetBody;
import com.ztesoft.zsmart.zmq.common.protocol.body.TopicList;
import com.ztesoft.zsmart.zmq.common.protocol.body.UnlockBatchRequestBody;
import com.ztesoft.zsmart.zmq.common.protocol.filtersrv.RegisterMessageFilterClassRequestHeader;
import com.ztesoft.zsmart.zmq.common.protocol.header.CloneGroupOffsetRequestHeader;
import com.ztesoft.zsmart.zmq.common.protocol.header.ConsumeMessageDirectlyResultRequestHeader;
import com.ztesoft.zsmart.zmq.common.protocol.header.ConsumerSendMsgBackRequestHeader;
import com.ztesoft.zsmart.zmq.common.protocol.header.CreateTopicRequestHeader;
import com.ztesoft.zsmart.zmq.common.protocol.header.DeleteSubscriptionGroupRequestHeader;
import com.ztesoft.zsmart.zmq.common.protocol.header.DeleteTopicRequestHeader;
import com.ztesoft.zsmart.zmq.common.protocol.header.EndTransactionRequestHeader;
import com.ztesoft.zsmart.zmq.common.protocol.header.GetConsumeStatsRequestHeader;
import com.ztesoft.zsmart.zmq.common.protocol.header.GetConsumerConnectionListRequestHeader;
import com.ztesoft.zsmart.zmq.common.protocol.header.GetConsumerListByGroupRequestHeader;
import com.ztesoft.zsmart.zmq.common.protocol.header.GetConsumerListByGroupResponseBody;
import com.ztesoft.zsmart.zmq.common.protocol.header.GetConsumerRunningInfoRequestHeader;
import com.ztesoft.zsmart.zmq.common.protocol.header.GetConsumerStatusRequestHeader;
import com.ztesoft.zsmart.zmq.common.protocol.header.GetEarliestMsgStoretimeRequestHeader;
import com.ztesoft.zsmart.zmq.common.protocol.header.GetEarliestMsgStoretimeResponseHeader;
import com.ztesoft.zsmart.zmq.common.protocol.header.GetMaxOffsetRequestHeader;
import com.ztesoft.zsmart.zmq.common.protocol.header.GetMaxOffsetResponseHeader;
import com.ztesoft.zsmart.zmq.common.protocol.header.GetMinOffsetRequestHeader;
import com.ztesoft.zsmart.zmq.common.protocol.header.GetMinOffsetResponseHeader;
import com.ztesoft.zsmart.zmq.common.protocol.header.GetProducerConnectionListRequestHeader;
import com.ztesoft.zsmart.zmq.common.protocol.header.GetTopicStatsInfoRequestHeader;
import com.ztesoft.zsmart.zmq.common.protocol.header.PullMessageRequestHeader;
import com.ztesoft.zsmart.zmq.common.protocol.header.PullMessageResponseHeader;
import com.ztesoft.zsmart.zmq.common.protocol.header.QueryConsumeTimeSpanRequestHeader;
import com.ztesoft.zsmart.zmq.common.protocol.header.QueryConsumerOffsetRequestHeader;
import com.ztesoft.zsmart.zmq.common.protocol.header.QueryConsumerOffsetResponseHeader;
import com.ztesoft.zsmart.zmq.common.protocol.header.QueryCorrectionOffsetHeader;
import com.ztesoft.zsmart.zmq.common.protocol.header.QueryMessageRequestHeader;
import com.ztesoft.zsmart.zmq.common.protocol.header.QueryTopicConsumeByWhoRequestHeader;
import com.ztesoft.zsmart.zmq.common.protocol.header.ResetOffsetRequestHeader;
import com.ztesoft.zsmart.zmq.common.protocol.header.SearchOffsetRequestHeader;
import com.ztesoft.zsmart.zmq.common.protocol.header.SearchOffsetResponseHeader;
import com.ztesoft.zsmart.zmq.common.protocol.header.SendMessageRequestHeader;
import com.ztesoft.zsmart.zmq.common.protocol.header.SendMessageRequestHeaderV2;
import com.ztesoft.zsmart.zmq.common.protocol.header.SendMessageResponseHeader;
import com.ztesoft.zsmart.zmq.common.protocol.header.UnregisterClientRequestHeader;
import com.ztesoft.zsmart.zmq.common.protocol.header.UpdateConsumerOffsetRequestHeader;
import com.ztesoft.zsmart.zmq.common.protocol.header.ViewBrokerStatsDataRequestHeader;
import com.ztesoft.zsmart.zmq.common.protocol.header.ViewMessageRequestHeader;
import com.ztesoft.zsmart.zmq.common.protocol.header.namesrv.DeleteKVConfigRequestHeader;
import com.ztesoft.zsmart.zmq.common.protocol.header.namesrv.GetKVConfigRequestHeader;
import com.ztesoft.zsmart.zmq.common.protocol.header.namesrv.GetKVConfigResponseHeader;
import com.ztesoft.zsmart.zmq.common.protocol.header.namesrv.GetKVListByNamespaceRequestHeader;
import com.ztesoft.zsmart.zmq.common.protocol.header.namesrv.GetRouteInfoRequestHeader;
import com.ztesoft.zsmart.zmq.common.protocol.header.namesrv.GetTopicsByClusterRequestHeader;
import com.ztesoft.zsmart.zmq.common.protocol.header.namesrv.PutKVConfigRequestHeader;
import com.ztesoft.zsmart.zmq.common.protocol.header.namesrv.WipeWritePermOfBrokerRequestHeader;
import com.ztesoft.zsmart.zmq.common.protocol.header.namesrv.WipeWritePermOfBrokerResponseHeader;
import com.ztesoft.zsmart.zmq.common.protocol.heartbeat.ConsumerData;
import com.ztesoft.zsmart.zmq.common.protocol.heartbeat.HeartbeatData;
import com.ztesoft.zsmart.zmq.common.protocol.heartbeat.ProducerData;
import com.ztesoft.zsmart.zmq.common.protocol.heartbeat.SubscriptionData;
import com.ztesoft.zsmart.zmq.common.protocol.route.TopicRouteData;
import com.ztesoft.zsmart.zmq.common.subscription.SubscriptionGroupConfig;
import com.ztesoft.zsmart.zmq.remoting.InvokeCallback;
import com.ztesoft.zsmart.zmq.remoting.RPCHook;
import com.ztesoft.zsmart.zmq.remoting.RemotingClient;
import com.ztesoft.zsmart.zmq.remoting.common.RemotingUtil;
import com.ztesoft.zsmart.zmq.remoting.exception.RemotingCommandException;
import com.ztesoft.zsmart.zmq.remoting.exception.RemotingConnectException;
import com.ztesoft.zsmart.zmq.remoting.exception.RemotingException;
import com.ztesoft.zsmart.zmq.remoting.exception.RemotingSendRequestException;
import com.ztesoft.zsmart.zmq.remoting.exception.RemotingTimeoutException;
import com.ztesoft.zsmart.zmq.remoting.exception.RemotingTooMuchRequestException;
import com.ztesoft.zsmart.zmq.remoting.netty.NettyClientConfig;
import com.ztesoft.zsmart.zmq.remoting.netty.NettyRemotingClient;
import com.ztesoft.zsmart.zmq.remoting.netty.ResponseFuture;
import com.ztesoft.zsmart.zmq.remoting.protocol.RemotingCommand;
import com.ztesoft.zsmart.zmq.remoting.protocol.RemotingSerializable;

public class MQClientAPIImpl {
    static {
        System.setProperty(RemotingCommand.RemotingVersionKey, Integer.toString(MQVersion.CurrentVersion));
    }

    private final static Logger log = ClientLogger.getLog();

    private final RemotingClient remotingClient;

    private final TopAddressing topAddressing = new TopAddressing(MixAll.WS_ADDR);

    private final ClientRemotingProcessor clientRemotingProcessor;

    private String nameSrvAddr = null;

    private String projectGroupPrefix;

    public MQClientAPIImpl(final NettyClientConfig nettyClientConfig,
        final ClientRemotingProcessor clientRemotingProcessor, RPCHook rpcHook) {
        this.remotingClient = new NettyRemotingClient(nettyClientConfig);
        this.clientRemotingProcessor = clientRemotingProcessor;
        this.remotingClient.registerRPCHook(rpcHook);

        this.remotingClient.registerProcessor(RequestCode.CHECK_TRANSACTION_STATE, clientRemotingProcessor, null);
        this.remotingClient.registerProcessor(RequestCode.NOTIFY_CONSUMER_IDS_CHANGED, this.clientRemotingProcessor,
            null);

        this.remotingClient.registerProcessor(RequestCode.RESET_CONSUMER_CLIENT_OFFSET, this.clientRemotingProcessor,
            null);

        this.remotingClient.registerProcessor(RequestCode.GET_CONSUMER_STATUS_FROM_CLIENT,
            this.clientRemotingProcessor, null);

        this.remotingClient
            .registerProcessor(RequestCode.GET_CONSUMER_RUNNING_INFO, this.clientRemotingProcessor, null);

        this.remotingClient.registerProcessor(RequestCode.CONSUME_MESSAGE_DIRECTLY, this.clientRemotingProcessor, null);
    }

    public MQClientAPIImpl(final NettyClientConfig nettyClientConfig,
        final ClientRemotingProcessor clientRemotingProcessor) {
        this(nettyClientConfig, clientRemotingProcessor, null);
    }

    public List<String> getNameServerAddressList() {
        return this.remotingClient.getNameServerAddressList();
    }

    public RemotingClient getRemotingClient() {
        return remotingClient;
    }

    public String fetchNameServerAddr() {
        try {
            String addrs = this.topAddressing.fetchNSAddr();
            if (addrs != null) {
                if (!addrs.equals(this.nameSrvAddr)) {
                    log.info("name server address changed, old: " + this.nameSrvAddr + " new: " + addrs);
                    this.updateNameServerAddressList(addrs);
                    this.nameSrvAddr = addrs;
                    return nameSrvAddr;
                }
            }
        }
        catch (Exception e) {
            log.error("fetchNameServerAddr Exception", e);
        }
        return nameSrvAddr;
    }

    public void updateNameServerAddressList(String addrs) {
        List<String> lst = new ArrayList<String>();
        String[] addrArray = addrs.split(";");
        if (addrArray != null) {
            for (String addr : addrArray) {
                lst.add(addr);
            }
            this.remotingClient.updateNameServerAddressList(lst);
        }

    }

    public void start() {
        this.remotingClient.start();

        // print app info
        try {
            String localAddress = RemotingUtil.getLocalAddress();
            projectGroupPrefix = this.getProjectGroupByIp(localAddress, 3000);
            log.info("The client[{}] in project group: {}", localAddress, projectGroupPrefix);
        }
        catch (Exception e) {
        }
    }

    public String getProjectGroupByIp(String ip, final long timeoutMillis) throws RemotingException, MQClientException,
        InterruptedException {
        return getKVConfigValue(NamesrvUtil.NAMESPACE_PROJECT_CONFIG, ip, timeoutMillis);
    }

    public void shutdown() {
        this.remotingClient.shutdown();
    }

    /**
     * 创建订阅组: <br>
     * 
     * @author wang.jun<br>
     * @taskId <br>
     * @param addr
     * @param config
     * @param timeoutMillis <br>
     * @throws InterruptedException
     * @throws RemotingTimeoutException
     * @throws RemotingSendRequestException
     * @throws RemotingConnectException
     * @throws MQClientException
     */
    public void createSubscriptionGroup(final String addr, final SubscriptionGroupConfig config,//
        final long timeoutMillis) throws RemotingConnectException, RemotingSendRequestException,
        RemotingTimeoutException, InterruptedException, MQClientException {
        if (!UtilAll.isBlank(projectGroupPrefix)) {
            config.setGroupName(VirtualEnvUtil.buildWithProjectGroup(config.getGroupName(), projectGroupPrefix));
        }

        RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.UPDATE_AND_CREATE_SUBSCRIPTIONGROUP,
            null);

        request.setBody(RemotingSerializable.encode(config));

        RemotingCommand response = this.remotingClient.invokeSync(addr, request, timeoutMillis);

        assert response != null;
        switch (response.getCode()) {
            case ResponseCode.SUCCESS: {
                return;
            }
            default:
                break;
        }

        throw new MQClientException(response.getCode(), response.getRemark());
    }

    /**
     * 创建topic: <br>
     * 
     * @author wang.jun<br>
     * @taskId <br>
     * @param addr
     * @param defaultTopic
     * @param topicConfig
     * @param timeoutMillis <br>
     * @throws InterruptedException
     * @throws RemotingTimeoutException
     * @throws RemotingSendRequestException
     * @throws RemotingConnectException
     * @throws MQClientException
     */
    public void createTopic(final String addr, final String defaultTopic, final TopicConfig topicConfig,
        final long timeoutMillis) throws RemotingConnectException, RemotingSendRequestException,
        RemotingTimeoutException, InterruptedException, MQClientException {
        String topicWithProjectGroup = topicConfig.getTopicName();
        if (!UtilAll.isBlank(projectGroupPrefix)) {
            topicWithProjectGroup = VirtualEnvUtil
                .buildWithProjectGroup(topicConfig.getTopicName(), projectGroupPrefix);
        }

        CreateTopicRequestHeader requestHeader = new CreateTopicRequestHeader();
        requestHeader.setTopic(topicWithProjectGroup);
        requestHeader.setDefaultTopic(defaultTopic);
        requestHeader.setReadQueueNums(topicConfig.getReadQueueNums());
        requestHeader.setWriteQueueNums(topicConfig.getWriteQueueNums());
        requestHeader.setPerm(topicConfig.getPerm());
        requestHeader.setTopicFilterType(topicConfig.getTopicFilterType().name());
        requestHeader.setTopicSysFlag(topicConfig.getTopicSysFlag());
        requestHeader.setOrder(topicConfig.isOrder());

        RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.UPDATE_AND_CREATE_TOPIC,
            requestHeader);

        RemotingCommand response = this.remotingClient.invokeSync(addr, request, timeoutMillis);

        assert response != null;
        switch (response.getCode()) {
            case ResponseCode.SUCCESS: {
                return;
            }
            default:
                break;
        }

        throw new MQClientException(response.getCode(), response.getRemark());

    }

    public static boolean sendSmartMsg = //
    Boolean.parseBoolean(System.getProperty("com.ztesoft.zmq.client.sendSmartMsg", "true"));

    /**
     * 发送消息: <br>
     * 
     * @author wang.jun<br>
     * @taskId <br>
     * @param addr
     * @param brokerName
     * @param msg
     * @param requestHeader
     * @param timeoutMillis
     * @param communicationMode
     * @param sendCallback
     * @return <br>
     * @throws InterruptedException
     * @throws RemotingSendRequestException
     * @throws RemotingTimeoutException
     * @throws RemotingTooMuchRequestException
     * @throws RemotingConnectException
     * @throws MQBrokerException
     * @throws RemotingCommandException
     */
    public SendResult sendMessage(//
        final String addr,// 1
        final String brokerName,// 2
        final Message msg,// 3
        final SendMessageRequestHeader requestHeader,// 4
        final long timeoutMillis,// 5
        final CommunicationMode communicationMode,// 6
        final SendCallback sendCallback// 7
    ) throws RemotingConnectException, RemotingTooMuchRequestException, RemotingTimeoutException,
        RemotingSendRequestException, InterruptedException, MQBrokerException, RemotingCommandException {
        if (!UtilAll.isBlank(projectGroupPrefix)) {
            msg.setTopic(VirtualEnvUtil.buildWithProjectGroup(msg.getTags(), projectGroupPrefix));
            requestHeader.setProducerGroup(VirtualEnvUtil.buildWithProjectGroup(requestHeader.getProducerGroup(),
                projectGroupPrefix));
            requestHeader.setTopic(VirtualEnvUtil.buildWithProjectGroup(requestHeader.getTopic(), projectGroupPrefix));
        }

        RemotingCommand request = null;
        if (sendSmartMsg) {
            SendMessageRequestHeaderV2 requestHeaderV2 = SendMessageRequestHeaderV2
                .createSendMessageRequestHeaderV2(requestHeader);
            request = RemotingCommand.createRequestCommand(RequestCode.SEND_MESSAGE_V2, requestHeaderV2);
        }
        else {
            request = RemotingCommand.createRequestCommand(RequestCode.SEND_MESSAGE, requestHeader);
        }

        request.setBody(msg.getBody());
        switch (communicationMode) {
            case ONEWAY:
                this.remotingClient.invokeOneway(addr, request, timeoutMillis);
                break;
            case ASYNC:
                this.sendMessageAsync(addr, brokerName, msg, timeoutMillis, request, sendCallback);
                return null;
            case SYNC:
                return this.sendMessageSync(addr, brokerName, msg, timeoutMillis, request);
            default:
                assert false;
                break;
        }

        return null;
    }

    /**
     * 同步消息发送: <br>
     * 
     * @author wang.jun<br>
     * @taskId <br>
     * @param addr
     * @param brokerName
     * @param msg
     * @param timeoutMillis
     * @param request
     * @return <br>
     * @throws InterruptedException
     * @throws RemotingTimeoutException
     * @throws RemotingSendRequestException
     * @throws RemotingConnectException
     * @throws MQBrokerException
     * @throws RemotingCommandException
     */
    private SendResult sendMessageSync(String addr, String brokerName, Message msg, long timeoutMillis,
        RemotingCommand request) throws RemotingConnectException, RemotingSendRequestException,
        RemotingTimeoutException, InterruptedException, MQBrokerException, RemotingCommandException {
        RemotingCommand response = this.remotingClient.invokeSync(addr, request, timeoutMillis);
        assert response != null;
        return this.processSendResponse(brokerName, msg, response);
    }

    /**
     * 异步发送消息: <br>
     * 
     * @author wang.jun<br>
     * @taskId <br>
     * @param addr
     * @param brokerName
     * @param msg
     * @param timeoutMillis
     * @param request
     * @param sendCallback <br>
     * @throws InterruptedException
     * @throws RemotingSendRequestException
     * @throws RemotingTimeoutException
     * @throws RemotingTooMuchRequestException
     * @throws RemotingConnectException
     */
    private void sendMessageAsync(String addr, final String brokerName, final Message msg, long timeoutMillis,
        RemotingCommand request, final SendCallback sendCallback) throws RemotingConnectException,
        RemotingTooMuchRequestException, RemotingTimeoutException, RemotingSendRequestException, InterruptedException {
        this.remotingClient.invokeAsync(addr, request, timeoutMillis, new InvokeCallback() {

            @Override
            public void operationComplete(ResponseFuture responseFuture) {
                if (sendCallback == null) {
                    return;
                }

                RemotingCommand response = responseFuture.getResponseCommand();
                if (response != null) {
                    try {
                        SendResult sendResult = MQClientAPIImpl.this.processSendResponse(brokerName, msg, response);
                        assert sendResult != null;
                        sendCallback.onSuccess(sendResult);
                    }
                    catch (Exception e) {
                        sendCallback.onException(e);
                    }
                }
                else {
                    if (!responseFuture.isSendRequestOK()) {
                        sendCallback.onException(new MQClientException("send request failed", responseFuture.getCause()));
                    }
                    else if (responseFuture.isTimeout()) {
                        sendCallback.onException(new MQClientException("wait response timeout "
                            + responseFuture.getTimeoutMillis() + "ms", responseFuture.getCause()));
                    }
                    else {
                        sendCallback.onException(new MQClientException("unknow reseaon", responseFuture.getCause()));
                    }
                }

            }
        });

    }

    /**
     * 发送消息结果处理: <br>
     * 
     * @author wang.jun<br>
     * @taskId <br>
     * @param brokerName
     * @param msg
     * @param response
     * @return <br>
     * @throws MQBrokerException
     * @throws RemotingCommandException
     */
    protected SendResult processSendResponse(String brokerName, Message msg, RemotingCommand response)
        throws MQBrokerException, RemotingCommandException {
        switch (response.getCode()) {
            case ResponseCode.FLUSH_DISK_TIMEOUT:
            case ResponseCode.FLUSH_SLAVE_TIMEOUT:
            case ResponseCode.SLAVE_NOT_AVAILABLE: {
                // TODO LOG
            }
            case ResponseCode.SUCCESS: {
                SendStatus sendStatus = SendStatus.SEND_OK;
                switch (response.getCode()) {
                    case ResponseCode.FLUSH_DISK_TIMEOUT:
                        sendStatus = SendStatus.FLUSH_DISK_TIMEOUT;
                        break;
                    case ResponseCode.FLUSH_SLAVE_TIMEOUT:
                        sendStatus = SendStatus.FLUSH_SLAVE_TIMEOUT;
                        break;
                    case ResponseCode.SLAVE_NOT_AVAILABLE:
                        sendStatus = SendStatus.SLAVE_NOT_AVAILABLE;
                        break;
                    case ResponseCode.SUCCESS:
                        sendStatus = SendStatus.SEND_OK;
                        break;
                    default:
                        assert false;
                        break;
                }

                SendMessageResponseHeader responseHeader = (SendMessageResponseHeader) response
                    .decodeCommandCustomHeader(SendMessageResponseHeader.class);

                MessageQueue messageQueue = new MessageQueue(msg.getTopic(), brokerName, responseHeader.getQueueId());

                SendResult sendResult = new SendResult(sendStatus, responseHeader.getMsgId(), messageQueue,
                    responseHeader.getQueueOffset(), brokerName);

                sendResult.setTransactionId(responseHeader.getTransactionId());

                return sendResult;
            }
            default:
                break;
        }
        throw new MQBrokerException(response.getCode(), response.getRemark());

    }

    /**
     * 拉取消息: <br>
     * 
     * @author wang.jun<br>
     * @taskId <br>
     * @param addr
     * @param requestHeader
     * @param timeoutMillis
     * @param communicationMode
     * @param pullCallback
     * @return <br>
     * @throws InterruptedException
     * @throws RemotingSendRequestException
     * @throws RemotingTimeoutException
     * @throws RemotingTooMuchRequestException
     * @throws RemotingConnectException
     * @throws MQBrokerException
     * @throws RemotingCommandException
     */
    public PullResult pullMessage(//
        final String addr,//
        final PullMessageRequestHeader requestHeader,//
        final long timeoutMillis,//
        final CommunicationMode communicationMode,//
        final PullCallback pullCallback) throws RemotingConnectException, RemotingTooMuchRequestException,
        RemotingTimeoutException, RemotingSendRequestException, InterruptedException, RemotingCommandException,
        MQBrokerException {
        if (!UtilAll.isBlank(projectGroupPrefix)) {
            requestHeader.setConsumerGroup(VirtualEnvUtil.buildWithProjectGroup(requestHeader.getConsumerGroup(),
                projectGroupPrefix));
            requestHeader.setTopic(VirtualEnvUtil.buildWithProjectGroup(requestHeader.getTopic(), projectGroupPrefix));
        }

        RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.PULL_MESSAGE, requestHeader);

        switch (communicationMode) {

            case ASYNC:
                this.pullMessageAsync(addr, request, timeoutMillis, pullCallback);
                return null;
            case SYNC:
                return this.pullMessageSync(addr, request, timeoutMillis);
            default:
                assert false;
                return null;
        }
    }

    /**
     * 异步拉取消息: <br>
     * 
     * @author wang.jun<br>
     * @taskId <br>
     * @param addr
     * @param request
     * @param timeoutMillis
     * @param pullCallback <br>
     * @throws InterruptedException
     * @throws RemotingSendRequestException
     * @throws RemotingTimeoutException
     * @throws RemotingTooMuchRequestException
     * @throws RemotingConnectException
     */
    private void pullMessageAsync(//
        final String addr,// 1
        final RemotingCommand request,//
        final long timeoutMillis,//
        final PullCallback pullCallback//
    ) throws RemotingConnectException, RemotingTooMuchRequestException, RemotingTimeoutException,
        RemotingSendRequestException, InterruptedException {
        this.remotingClient.invokeAsync(addr, request, timeoutMillis, new InvokeCallback() {

            @Override
            public void operationComplete(ResponseFuture responseFuture) {

                RemotingCommand response = responseFuture.getResponseCommand();
                if (response != null) {
                    try {
                        PullResult pullResult = MQClientAPIImpl.this.processPullResponse(response);
                        assert pullResult != null;
                        pullCallback.onSuccess(pullResult);
                    }
                    catch (Exception e) {
                        pullCallback.onException(e);
                    }
                }
                else {
                    if (!responseFuture.isSendRequestOK()) {
                        pullCallback.onException(new MQClientException("send request failed", responseFuture.getCause()));
                    }
                    else if (responseFuture.isTimeout()) {
                        pullCallback.onException(new MQClientException("wait response timeout "
                            + responseFuture.getTimeoutMillis() + "ms", responseFuture.getCause()));
                    }
                    else {
                        pullCallback.onException(new MQClientException("unknow reseaon", responseFuture.getCause()));
                    }
                }
            }
        });

    }

    /**
     * 同步拉取消息: <br>
     * 
     * @author wang.jun<br>
     * @taskId <br>
     * @param addr
     * @param request
     * @param timeoutMillis
     * @return <br>
     * @throws InterruptedException
     * @throws RemotingTimeoutException
     * @throws RemotingSendRequestException
     * @throws RemotingConnectException
     * @throws MQBrokerException
     * @throws RemotingCommandException
     */
    private PullResult pullMessageSync(String addr, RemotingCommand request, long timeoutMillis)
        throws RemotingConnectException, RemotingSendRequestException, RemotingTimeoutException, InterruptedException,
        RemotingCommandException, MQBrokerException {
        RemotingCommand response = this.remotingClient.invokeSync(addr, request, timeoutMillis);
        assert response != null;
        return this.processPullResponse(response);

    }

    /**
     * 拉消息结果处理: <br>
     * 
     * @author wang.jun<br>
     * @taskId <br>
     * @param response
     * @return <br>
     * @throws MQBrokerException
     * @throws RemotingCommandException
     */
    private PullResult processPullResponse(RemotingCommand response) throws MQBrokerException, RemotingCommandException {
        PullStatus pullStatus = PullStatus.NO_NEW_MSG;

        switch (response.getCode()) {
            case ResponseCode.SUCCESS:
                pullStatus = PullStatus.FOUND;
                break;
            case ResponseCode.PULL_NOT_FOUND:
                pullStatus = PullStatus.NO_NEW_MSG;
                break;
            case ResponseCode.PULL_RETRY_IMMEDIATELY:
                pullStatus = PullStatus.NO_MATCHED_MSG;
                break;
            case ResponseCode.PULL_OFFSET_MOVED:
                pullStatus = PullStatus.OFFSET_ILLEGAL;
                break;
            default:
                throw new MQBrokerException(response.getCode(), response.getRemark());
        }

        PullMessageResponseHeader responseHeader = (PullMessageResponseHeader) response
            .decodeCommandCustomHeader(PullMessageResponseHeader.class);

        return new PullResultExt(pullStatus, responseHeader.getNextBeginOffset(), responseHeader.getMinOffset(),
            responseHeader.getMaxOffset(), null, responseHeader.getSuggestWhichBrokerId(), response.getBody());

    }

    public MessageExt viewMessage(final String addr, final long phyoffset, final long timeoutMillis)
        throws RemotingException, MQBrokerException, InterruptedException {
        ViewMessageRequestHeader requestHeader = new ViewMessageRequestHeader();
        requestHeader.setOffset(phyoffset);
        RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.VIEW_MESSAGE_BY_ID, requestHeader);

        RemotingCommand response = this.remotingClient.invokeSync(addr, request, timeoutMillis);
        assert response != null;
        switch (response.getCode()) {
            case ResponseCode.SUCCESS: {
                ByteBuffer byteBuffer = ByteBuffer.wrap(response.getBody());
                MessageExt messageExt = MessageDecoder.decode(byteBuffer);
                if (!UtilAll.isBlank(projectGroupPrefix)) {
                    messageExt.setTopic(VirtualEnvUtil.clearProjectGroup(messageExt.getTopic(), projectGroupPrefix));
                }
                return messageExt;
            }
            default:
                break;
        }

        throw new MQBrokerException(response.getCode(), response.getRemark());
    }

    /**
     * 查询消费进度: <br>
     * 
     * @author wang.jun<br>
     * @taskId <br>
     * @param addr
     * @param topic
     * @param queueId
     * @param timestamp
     * @param timeoutMillis
     * @return <br>
     * @throws InterruptedException
     * @throws RemotingTimeoutException
     * @throws RemotingSendRequestException
     * @throws RemotingConnectException
     * @throws RemotingCommandException
     * @throws MQBrokerException
     */
    public long searchOffset(final String addr, final String topic, final int queueId, final long timestamp,
        final long timeoutMillis) throws RemotingConnectException, RemotingSendRequestException,
        RemotingTimeoutException, InterruptedException, RemotingCommandException, MQBrokerException {
        String topicWithProjectGroup = topic;
        if (!UtilAll.isBlank(projectGroupPrefix)) {
            topicWithProjectGroup = VirtualEnvUtil.buildWithProjectGroup(topic, projectGroupPrefix);
        }

        SearchOffsetRequestHeader requestHeader = new SearchOffsetRequestHeader();
        requestHeader.setTopic(topicWithProjectGroup);
        requestHeader.setQueueId(queueId);
        requestHeader.setTimestamp(timestamp);

        RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.SEARCH_OFFSET_BY_TIMESTAMP,
            requestHeader);

        RemotingCommand response = this.remotingClient.invokeSync(addr, request, timeoutMillis);

        assert response != null;

        switch (response.getCode()) {
            case ResponseCode.SUCCESS:
                SearchOffsetResponseHeader responseHeader = (SearchOffsetResponseHeader) response
                    .decodeCommandCustomHeader(SearchOffsetResponseHeader.class);
                return responseHeader.getOffset();

            default:
                break;
        }

        throw new MQBrokerException(response.getCode(), response.getRemark());
    }

    /**
     * Description: <br>
     * 
     * @author wang.jun<br>
     * @taskId <br>
     * @param addr
     * @param topic
     * @param queueId
     * @param timeoutMillis
     * @return
     * @throws RemotingException
     * @throws MQBrokerException
     * @throws InterruptedException <br>
     */
    public long getMaxOffset(final String addr, final String topic, final int queueId, final long timeoutMillis)
        throws RemotingException, MQBrokerException, InterruptedException {
        String topicWithProjectGroup = topic;
        if (!UtilAll.isBlank(projectGroupPrefix)) {
            topicWithProjectGroup = VirtualEnvUtil.buildWithProjectGroup(topic, projectGroupPrefix);
        }

        GetMaxOffsetRequestHeader requestHeader = new GetMaxOffsetRequestHeader();
        requestHeader.setTopic(topicWithProjectGroup);
        requestHeader.setQueueId(queueId);
        RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.GET_MAX_OFFSET, requestHeader);

        RemotingCommand response = this.remotingClient.invokeSync(addr, request, timeoutMillis);
        assert response != null;
        switch (response.getCode()) {
            case ResponseCode.SUCCESS: {
                GetMaxOffsetResponseHeader responseHeader = (GetMaxOffsetResponseHeader) response
                    .decodeCommandCustomHeader(GetMaxOffsetResponseHeader.class);

                return responseHeader.getOffset();
            }
            default:
                break;
        }

        throw new MQBrokerException(response.getCode(), response.getRemark());

    }

    /**
     * 根据consumergroup 获consumer列表: <br>
     * 
     * @author wang.jun<br>
     * @taskId <br>
     * @param addr
     * @param consumerGroup
     * @param timeoutMillis
     * @return
     * @throws RemotingConnectException
     * @throws RemotingSendRequestException
     * @throws RemotingTimeoutException
     * @throws MQBrokerException
     * @throws InterruptedException <br>
     */
    public List<String> getConsumerIdListByGroup(//
        final String addr, //
        final String consumerGroup, //
        final long timeoutMillis) throws RemotingConnectException, RemotingSendRequestException,
        RemotingTimeoutException, MQBrokerException, InterruptedException {
        String consumerGroupWithProjectGroup = consumerGroup;
        if (!UtilAll.isBlank(projectGroupPrefix)) {
            consumerGroupWithProjectGroup = VirtualEnvUtil.buildWithProjectGroup(consumerGroup, projectGroupPrefix);
        }

        GetConsumerListByGroupRequestHeader requestHeader = new GetConsumerListByGroupRequestHeader();
        requestHeader.setConsumerGroup(consumerGroupWithProjectGroup);
        RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.GET_CONSUMER_LIST_BY_GROUP,
            requestHeader);

        RemotingCommand response = this.remotingClient.invokeSync(addr, request, timeoutMillis);
        assert response != null;
        switch (response.getCode()) {
            case ResponseCode.SUCCESS: {
                if (response.getBody() != null) {
                    GetConsumerListByGroupResponseBody body = GetConsumerListByGroupResponseBody.decode(
                        response.getBody(), GetConsumerListByGroupResponseBody.class);
                    return body.getConsumerIdList();
                }
            }
            default:
                break;
        }

        throw new MQBrokerException(response.getCode(), response.getRemark());
    }

    public long getMinOffset(final String addr, final String topic, final int queueId, final long timeoutMillis)
        throws RemotingException, MQBrokerException, InterruptedException {
        String topicWithProjectGroup = topic;
        if (!UtilAll.isBlank(projectGroupPrefix)) {
            topicWithProjectGroup = VirtualEnvUtil.buildWithProjectGroup(topic, projectGroupPrefix);
        }

        GetMinOffsetRequestHeader requestHeader = new GetMinOffsetRequestHeader();
        requestHeader.setTopic(topicWithProjectGroup);
        requestHeader.setQueueId(queueId);
        RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.GET_MIN_OFFSET, requestHeader);

        RemotingCommand response = this.remotingClient.invokeSync(addr, request, timeoutMillis);
        assert response != null;
        switch (response.getCode()) {
            case ResponseCode.SUCCESS: {
                GetMinOffsetResponseHeader responseHeader = (GetMinOffsetResponseHeader) response
                    .decodeCommandCustomHeader(GetMinOffsetResponseHeader.class);

                return responseHeader.getOffset();
            }
            default:
                break;
        }

        throw new MQBrokerException(response.getCode(), response.getRemark());
    }

    public long getEarliestMsgStoretime(final String addr, final String topic, final int queueId,
        final long timeoutMillis) throws RemotingException, MQBrokerException, InterruptedException {
        String topicWithProjectGroup = topic;
        if (!UtilAll.isBlank(projectGroupPrefix)) {
            topicWithProjectGroup = VirtualEnvUtil.buildWithProjectGroup(topic, projectGroupPrefix);
        }

        GetEarliestMsgStoretimeRequestHeader requestHeader = new GetEarliestMsgStoretimeRequestHeader();
        requestHeader.setTopic(topicWithProjectGroup);
        requestHeader.setQueueId(queueId);
        RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.GET_EARLIEST_MSG_STORETIME,
            requestHeader);

        RemotingCommand response = this.remotingClient.invokeSync(addr, request, timeoutMillis);
        assert response != null;
        switch (response.getCode()) {
            case ResponseCode.SUCCESS: {
                GetEarliestMsgStoretimeResponseHeader responseHeader = (GetEarliestMsgStoretimeResponseHeader) response
                    .decodeCommandCustomHeader(GetEarliestMsgStoretimeResponseHeader.class);

                return responseHeader.getTimestamp();
            }
            default:
                break;
        }

        throw new MQBrokerException(response.getCode(), response.getRemark());
    }

    public long queryConsumerOffset(//
        final String addr,//
        final QueryConsumerOffsetRequestHeader requestHeader,//
        final long timeoutMillis//
    ) throws RemotingException, MQBrokerException, InterruptedException {
        if (!UtilAll.isBlank(projectGroupPrefix)) {
            requestHeader.setConsumerGroup(VirtualEnvUtil.buildWithProjectGroup(requestHeader.getConsumerGroup(),
                projectGroupPrefix));
            requestHeader.setTopic(VirtualEnvUtil.buildWithProjectGroup(requestHeader.getTopic(), projectGroupPrefix));
        }

        RemotingCommand request = RemotingCommand
            .createRequestCommand(RequestCode.QUERY_CONSUMER_OFFSET, requestHeader);

        RemotingCommand response = this.remotingClient.invokeSync(addr, request, timeoutMillis);
        assert response != null;
        switch (response.getCode()) {
            case ResponseCode.SUCCESS: {
                QueryConsumerOffsetResponseHeader responseHeader = (QueryConsumerOffsetResponseHeader) response
                    .decodeCommandCustomHeader(QueryConsumerOffsetResponseHeader.class);

                return responseHeader.getOffset();
            }
            default:
                break;
        }

        throw new MQBrokerException(response.getCode(), response.getRemark());
    }

    public void updateConsumerOffset(//
        final String addr,//
        final UpdateConsumerOffsetRequestHeader requestHeader,//
        final long timeoutMillis//
    ) throws RemotingException, MQBrokerException, InterruptedException {
        if (!UtilAll.isBlank(projectGroupPrefix)) {
            requestHeader.setConsumerGroup(VirtualEnvUtil.buildWithProjectGroup(requestHeader.getConsumerGroup(),
                projectGroupPrefix));
            requestHeader.setTopic(VirtualEnvUtil.buildWithProjectGroup(requestHeader.getTopic(), projectGroupPrefix));
        }

        RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.UPDATE_CONSUMER_OFFSET,
            requestHeader);

        RemotingCommand response = this.remotingClient.invokeSync(addr, request, timeoutMillis);
        assert response != null;
        switch (response.getCode()) {
            case ResponseCode.SUCCESS: {
                return;
            }
            default:
                break;
        }

        throw new MQBrokerException(response.getCode(), response.getRemark());
    }

    public void updateConsumerOffsetOneway(//
        final String addr,//
        final UpdateConsumerOffsetRequestHeader requestHeader,//
        final long timeoutMillis//
    ) throws RemotingConnectException, RemotingTooMuchRequestException, RemotingTimeoutException,
        RemotingSendRequestException, InterruptedException {
        if (!UtilAll.isBlank(projectGroupPrefix)) {
            requestHeader.setConsumerGroup(VirtualEnvUtil.buildWithProjectGroup(requestHeader.getConsumerGroup(),
                projectGroupPrefix));
            requestHeader.setTopic(VirtualEnvUtil.buildWithProjectGroup(requestHeader.getTopic(), projectGroupPrefix));
        }

        RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.UPDATE_CONSUMER_OFFSET,
            requestHeader);

        this.remotingClient.invokeOneway(addr, request, timeoutMillis);
    }

    public void sendHearbeat(//
        final String addr,//
        final HeartbeatData heartbeatData,//
        final long timeoutMillis//
    ) throws RemotingException, MQBrokerException, InterruptedException {
        if (!UtilAll.isBlank(projectGroupPrefix)) {
            Set<ConsumerData> consumerDatas = heartbeatData.getConsumerDataSet();
            for (ConsumerData consumerData : consumerDatas) {
                consumerData.setGroupName(VirtualEnvUtil.buildWithProjectGroup(consumerData.getGroupName(),
                    projectGroupPrefix));
                Set<SubscriptionData> subscriptionDatas = consumerData.getSubscriptionDataSet();
                for (SubscriptionData subscriptionData : subscriptionDatas) {
                    subscriptionData.setTopic(VirtualEnvUtil.buildWithProjectGroup(subscriptionData.getTopic(),
                        projectGroupPrefix));
                }
            }
            Set<ProducerData> producerDatas = heartbeatData.getProducerDataSet();
            for (ProducerData producerData : producerDatas) {
                producerData.setGroupName(VirtualEnvUtil.buildWithProjectGroup(producerData.getGroupName(),
                    projectGroupPrefix));
            }
        }

        RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.HEART_BEAT, null);

        request.setBody(heartbeatData.encode());
        RemotingCommand response = this.remotingClient.invokeSync(addr, request, timeoutMillis);
        assert response != null;
        switch (response.getCode()) {
            case ResponseCode.SUCCESS: {
                return;
            }
            default:
                break;
        }

        throw new MQBrokerException(response.getCode(), response.getRemark());
    }

    public void unregisterClient(//
        final String addr,//
        final String clientID,//
        final String producerGroup,//
        final String consumerGroup,//
        final long timeoutMillis//
    ) throws RemotingException, MQBrokerException, InterruptedException {
        String producerGroupWithProjectGroup = producerGroup;
        String consumerGroupWithProjectGroup = consumerGroup;
        if (!UtilAll.isBlank(projectGroupPrefix)) {
            producerGroupWithProjectGroup = VirtualEnvUtil.buildWithProjectGroup(producerGroup, projectGroupPrefix);
            consumerGroupWithProjectGroup = VirtualEnvUtil.buildWithProjectGroup(consumerGroup, projectGroupPrefix);
        }

        final UnregisterClientRequestHeader requestHeader = new UnregisterClientRequestHeader();
        requestHeader.setClientID(clientID);
        requestHeader.setProducerGroup(producerGroupWithProjectGroup);
        requestHeader.setConsumerGroup(consumerGroupWithProjectGroup);
        RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.UNREGISTER_CLIENT, requestHeader);

        RemotingCommand response = this.remotingClient.invokeSync(addr, request, timeoutMillis);
        assert response != null;
        switch (response.getCode()) {
            case ResponseCode.SUCCESS: {
                return;
            }
            default:
                break;
        }

        throw new MQBrokerException(response.getCode(), response.getRemark());
    }

    public void endTransactionOneway(//
        final String addr,//
        final EndTransactionRequestHeader requestHeader,//
        final String remark,//
        final long timeoutMillis//
    ) throws RemotingException, MQBrokerException, InterruptedException {
        if (!UtilAll.isBlank(projectGroupPrefix)) {
            requestHeader.setProducerGroup(VirtualEnvUtil.buildWithProjectGroup(requestHeader.getProducerGroup(),
                projectGroupPrefix));
        }

        RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.END_TRANSACTION, requestHeader);

        request.setRemark(remark);
        this.remotingClient.invokeOneway(addr, request, timeoutMillis);
    }

    public void queryMessage(//
        final String addr,//
        final QueryMessageRequestHeader requestHeader,//
        final long timeoutMillis,//
        final InvokeCallback invokeCallback//
    ) throws RemotingException, MQBrokerException, InterruptedException {
        if (!UtilAll.isBlank(projectGroupPrefix)) {
            requestHeader.setTopic(VirtualEnvUtil.buildWithProjectGroup(requestHeader.getTopic(), projectGroupPrefix));
        }

        RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.QUERY_MESSAGE, requestHeader);

        this.remotingClient.invokeAsync(addr, request, timeoutMillis, invokeCallback);
    }

    public boolean registerClient(final String addr, final HeartbeatData heartbeat, final long timeoutMillis)
        throws RemotingException, InterruptedException {
        if (!UtilAll.isBlank(projectGroupPrefix)) {
            Set<ConsumerData> consumerDatas = heartbeat.getConsumerDataSet();
            for (ConsumerData consumerData : consumerDatas) {
                consumerData.setGroupName(VirtualEnvUtil.buildWithProjectGroup(consumerData.getGroupName(),
                    projectGroupPrefix));
                Set<SubscriptionData> subscriptionDatas = consumerData.getSubscriptionDataSet();
                for (SubscriptionData subscriptionData : subscriptionDatas) {
                    subscriptionData.setTopic(VirtualEnvUtil.buildWithProjectGroup(subscriptionData.getTopic(),
                        projectGroupPrefix));
                }
            }
            Set<ProducerData> producerDatas = heartbeat.getProducerDataSet();
            for (ProducerData producerData : producerDatas) {
                producerData.setGroupName(VirtualEnvUtil.buildWithProjectGroup(producerData.getGroupName(),
                    projectGroupPrefix));
            }
        }

        RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.HEART_BEAT, null);

        request.setBody(heartbeat.encode());
        RemotingCommand response = this.remotingClient.invokeSync(addr, request, timeoutMillis);
        return response.getCode() == ResponseCode.SUCCESS;
    }

    public void consumerSendMessageBack(//
        final String addr, //
        final MessageExt msg,//
        final String consumerGroup,//
        final int delayLevel,//
        final long timeoutMillis//
    ) throws RemotingException, MQBrokerException, InterruptedException {
        String consumerGroupWithProjectGroup = consumerGroup;
        if (!UtilAll.isBlank(projectGroupPrefix)) {
            consumerGroupWithProjectGroup = VirtualEnvUtil.buildWithProjectGroup(consumerGroup, projectGroupPrefix);
            msg.setTopic(VirtualEnvUtil.buildWithProjectGroup(msg.getTopic(), projectGroupPrefix));
        }

        ConsumerSendMsgBackRequestHeader requestHeader = new ConsumerSendMsgBackRequestHeader();
        RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.CONSUMER_SEND_MSG_BACK,
            requestHeader);

        requestHeader.setGroup(consumerGroupWithProjectGroup);
        requestHeader.setOriginTopic(msg.getTopic());
        requestHeader.setOffset(msg.getCommitLogOffset());
        requestHeader.setDelayLevel(delayLevel);
        requestHeader.setOriginMsgId(msg.getMsgId());

        RemotingCommand response = this.remotingClient.invokeSync(addr, request, timeoutMillis);
        assert response != null;
        switch (response.getCode()) {
            case ResponseCode.SUCCESS: {
                return;
            }
            default:
                break;
        }

        throw new MQBrokerException(response.getCode(), response.getRemark());
    }

    public Set<MessageQueue> lockBatchMQ(//
        final String addr,//
        final LockBatchRequestBody requestBody,//
        final long timeoutMillis) throws RemotingException, MQBrokerException, InterruptedException {
        if (!UtilAll.isBlank(projectGroupPrefix)) {
            requestBody.setConsumerGroup((VirtualEnvUtil.buildWithProjectGroup(requestBody.getConsumerGroup(),
                projectGroupPrefix)));
            Set<MessageQueue> messageQueues = requestBody.getMqSet();
            for (MessageQueue messageQueue : messageQueues) {
                messageQueue
                    .setTopic(VirtualEnvUtil.buildWithProjectGroup(messageQueue.getTopic(), projectGroupPrefix));
            }
        }

        RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.LOCK_BATCH_MQ, null);

        request.setBody(requestBody.encode());
        RemotingCommand response = this.remotingClient.invokeSync(addr, request, timeoutMillis);
        switch (response.getCode()) {
            case ResponseCode.SUCCESS: {
                LockBatchResponseBody responseBody = LockBatchResponseBody.decode(response.getBody(),
                    LockBatchResponseBody.class);
                Set<MessageQueue> messageQueues = responseBody.getLockOKMQSet();
                if (!UtilAll.isBlank(projectGroupPrefix)) {
                    for (MessageQueue messageQueue : messageQueues) {
                        messageQueue.setTopic(VirtualEnvUtil.clearProjectGroup(messageQueue.getTopic(),
                            projectGroupPrefix));
                    }
                }
                return messageQueues;
            }
            default:
                break;
        }

        throw new MQBrokerException(response.getCode(), response.getRemark());
    }

    public void unlockBatchMQ(//
        final String addr,//
        final UnlockBatchRequestBody requestBody,//
        final long timeoutMillis,//
        final boolean oneway//
    ) throws RemotingException, MQBrokerException, InterruptedException {
        if (!UtilAll.isBlank(projectGroupPrefix)) {
            requestBody.setConsumerGroup(VirtualEnvUtil.buildWithProjectGroup(requestBody.getConsumerGroup(),
                projectGroupPrefix));
            Set<MessageQueue> messageQueues = requestBody.getMqSet();
            for (MessageQueue messageQueue : messageQueues) {
                messageQueue
                    .setTopic(VirtualEnvUtil.buildWithProjectGroup(messageQueue.getTopic(), projectGroupPrefix));
            }
        }

        RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.UNLOCK_BATCH_MQ, null);

        request.setBody(requestBody.encode());

        if (oneway) {
            this.remotingClient.invokeOneway(addr, request, timeoutMillis);
        }
        else {
            RemotingCommand response = this.remotingClient.invokeSync(addr, request, timeoutMillis);
            switch (response.getCode()) {
                case ResponseCode.SUCCESS: {
                    return;
                }
                default:
                    break;
            }

            throw new MQBrokerException(response.getCode(), response.getRemark());
        }
    }

    public TopicStatsTable getTopicStatsInfo(final String addr, final String topic, final long timeoutMillis)
        throws InterruptedException, RemotingTimeoutException, RemotingSendRequestException, RemotingConnectException,
        MQBrokerException {
        String topicWithProjectGroup = topic;
        if (!UtilAll.isBlank(projectGroupPrefix)) {
            topicWithProjectGroup = VirtualEnvUtil.buildWithProjectGroup(topic, projectGroupPrefix);
        }

        GetTopicStatsInfoRequestHeader requestHeader = new GetTopicStatsInfoRequestHeader();
        requestHeader.setTopic(topicWithProjectGroup);

        RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.GET_TOPIC_STATS_INFO, requestHeader);

        RemotingCommand response = this.remotingClient.invokeSync(addr, request, timeoutMillis);
        switch (response.getCode()) {
            case ResponseCode.SUCCESS: {
                TopicStatsTable topicStatsTable = TopicStatsTable.decode(response.getBody(), TopicStatsTable.class);
                if (!UtilAll.isBlank(projectGroupPrefix)) {
                    HashMap<MessageQueue, TopicOffset> newTopicOffsetMap = new HashMap<MessageQueue, TopicOffset>();
                    for (Map.Entry<MessageQueue, TopicOffset> messageQueue : topicStatsTable.getOffsetTable()
                        .entrySet()) {
                        MessageQueue key = messageQueue.getKey();
                        key.setTopic(VirtualEnvUtil.clearProjectGroup(key.getTopic(), projectGroupPrefix));
                        newTopicOffsetMap.put(key, messageQueue.getValue());
                    }
                    topicStatsTable.setOffsetTable(newTopicOffsetMap);
                }
                return topicStatsTable;
            }
            default:
                break;
        }

        throw new MQBrokerException(response.getCode(), response.getRemark());
    }

    /**
     * 获取kv配置: <br>
     * 
     * @author wang.jun<br>
     * @taskId <br>
     * @param namespaceProjectConfig
     * @param ip
     * @param timeoutMillis
     * @return <br>
     * @throws InterruptedException
     * @throws RemotingTimeoutException
     * @throws RemotingSendRequestException
     * @throws RemotingConnectException
     * @throws RemotingCommandException
     * @throws MQClientException
     */
    private String getKVConfigValue(String namespace, String value, long timeoutMillis)
        throws RemotingConnectException, RemotingSendRequestException, RemotingTimeoutException, InterruptedException,
        RemotingCommandException, MQClientException {
        GetKVConfigRequestHeader requestHeader = new GetKVConfigRequestHeader();
        requestHeader.setNamespace(namespace);
        requestHeader.setKey(value);

        RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.GET_KV_CONFIG_BY_VALUE,
            requestHeader);

        RemotingCommand response = this.remotingClient.invokeSync(null, request, timeoutMillis);

        assert response != null;
        switch (response.getCode()) {
            case ResponseCode.SUCCESS:
                GetKVConfigResponseHeader responseHeader = (GetKVConfigResponseHeader) response
                    .decodeCommandCustomHeader(GetKVConfigResponseHeader.class);
                return responseHeader.getValue();

            default:
                break;
        }

        throw new MQClientException(response.getCode(), response.getRemark());
    }

    public ConsumeStats getConsumeStats(final String addr, final String consumerGroup, final long timeoutMillis)
        throws InterruptedException, RemotingTimeoutException, RemotingSendRequestException, RemotingConnectException,
        MQBrokerException {
        return getConsumeStats(addr, consumerGroup, null, timeoutMillis);
    }

    public ConsumeStats getConsumeStats(final String addr, final String consumerGroup, final String topic,
        final long timeoutMillis) throws InterruptedException, RemotingTimeoutException, RemotingSendRequestException,
        RemotingConnectException, MQBrokerException {
        String consumerGroupWithProjectGroup = consumerGroup;
        if (!UtilAll.isBlank(projectGroupPrefix)) {
            consumerGroupWithProjectGroup = VirtualEnvUtil.buildWithProjectGroup(consumerGroup, projectGroupPrefix);
        }

        GetConsumeStatsRequestHeader requestHeader = new GetConsumeStatsRequestHeader();
        requestHeader.setConsumerGroup(consumerGroupWithProjectGroup);
        requestHeader.setTopic(topic);

        RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.GET_CONSUME_STATS, requestHeader);

        RemotingCommand response = this.remotingClient.invokeSync(addr, request, timeoutMillis);
        switch (response.getCode()) {
            case ResponseCode.SUCCESS: {
                ConsumeStats consumeStats = ConsumeStats.decode(response.getBody(), ConsumeStats.class);
                if (!UtilAll.isBlank(projectGroupPrefix)) {
                    HashMap<MessageQueue, OffsetWrapper> newTopicOffsetMap = new HashMap<MessageQueue, OffsetWrapper>();
                    for (Map.Entry<MessageQueue, OffsetWrapper> messageQueue : consumeStats.getOffsetTable().entrySet()) {
                        MessageQueue key = messageQueue.getKey();
                        key.setTopic(VirtualEnvUtil.clearProjectGroup(key.getTopic(), projectGroupPrefix));
                        newTopicOffsetMap.put(key, messageQueue.getValue());
                    }
                    consumeStats.setOffsetTable(newTopicOffsetMap);
                }

                return consumeStats;
            }
            default:
                break;
        }

        throw new MQBrokerException(response.getCode(), response.getRemark());
    }

    public ProducerConnection getProducerConnectionList(final String addr, final String producerGroup,
        final long timeoutMillis) throws RemotingConnectException, RemotingSendRequestException,
        RemotingTimeoutException, InterruptedException, MQBrokerException {
        String producerGroupWithProjectGroup = producerGroup;
        if (!UtilAll.isBlank(projectGroupPrefix)) {
            producerGroupWithProjectGroup = VirtualEnvUtil.buildWithProjectGroup(producerGroup, projectGroupPrefix);
        }

        GetProducerConnectionListRequestHeader requestHeader = new GetProducerConnectionListRequestHeader();
        requestHeader.setProducerGroup(producerGroupWithProjectGroup);

        RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.GET_PRODUCER_CONNECTION_LIST,
            requestHeader);

        RemotingCommand response = this.remotingClient.invokeSync(addr, request, timeoutMillis);
        switch (response.getCode()) {
            case ResponseCode.SUCCESS: {
                return ProducerConnection.decode(response.getBody(), ProducerConnection.class);
            }
            default:
                break;
        }

        throw new MQBrokerException(response.getCode(), response.getRemark());
    }

    public ConsumerConnection getConsumerConnectionList(final String addr, final String consumerGroup,
        final long timeoutMillis) throws RemotingConnectException, RemotingSendRequestException,
        RemotingTimeoutException, InterruptedException, MQBrokerException {
        String consumerGroupWithProjectGroup = consumerGroup;
        if (!UtilAll.isBlank(projectGroupPrefix)) {
            consumerGroupWithProjectGroup = VirtualEnvUtil.buildWithProjectGroup(consumerGroup, projectGroupPrefix);
        }

        GetConsumerConnectionListRequestHeader requestHeader = new GetConsumerConnectionListRequestHeader();
        requestHeader.setConsumerGroup(consumerGroupWithProjectGroup);

        RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.GET_CONSUMER_CONNECTION_LIST,
            requestHeader);

        RemotingCommand response = this.remotingClient.invokeSync(addr, request, timeoutMillis);
        switch (response.getCode()) {
            case ResponseCode.SUCCESS: {
                ConsumerConnection consumerConnection = ConsumerConnection.decode(response.getBody(),
                    ConsumerConnection.class);
                if (!UtilAll.isBlank(projectGroupPrefix)) {
                    ConcurrentHashMap<String, SubscriptionData> subscriptionDataConcurrentHashMap = consumerConnection
                        .getSubscriptionTable();
                    for (Map.Entry<String, SubscriptionData> subscriptionDataEntry : subscriptionDataConcurrentHashMap
                        .entrySet()) {
                        SubscriptionData subscriptionData = subscriptionDataEntry.getValue();
                        subscriptionDataEntry.getValue().setTopic(
                            VirtualEnvUtil.clearProjectGroup(subscriptionData.getTopic(), projectGroupPrefix));
                    }
                }
                return consumerConnection;
            }
            default:
                break;
        }

        throw new MQBrokerException(response.getCode(), response.getRemark());
    }

    public KVTable getBrokerRuntimeInfo(final String addr, final long timeoutMillis) throws RemotingConnectException,
        RemotingSendRequestException, RemotingTimeoutException, InterruptedException, MQBrokerException {

        RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.GET_BROKER_RUNTIME_INFO, null);

        RemotingCommand response = this.remotingClient.invokeSync(addr, request, timeoutMillis);
        switch (response.getCode()) {
            case ResponseCode.SUCCESS: {
                return KVTable.decode(response.getBody(), KVTable.class);
            }
            default:
                break;
        }

        throw new MQBrokerException(response.getCode(), response.getRemark());
    }

    public void updateBrokerConfig(final String addr, final Properties properties, final long timeoutMillis)
        throws RemotingConnectException, RemotingSendRequestException, RemotingTimeoutException, InterruptedException,
        MQBrokerException, UnsupportedEncodingException {

        RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.UPDATE_BROKER_CONFIG, null);

        String str = MixAll.properties2String(properties);
        if (str != null && str.length() > 0) {
            request.setBody(str.getBytes(MixAll.DEFAULT_CHARSET));
            RemotingCommand response = this.remotingClient.invokeSync(addr, request, timeoutMillis);
            switch (response.getCode()) {
                case ResponseCode.SUCCESS: {
                    return;
                }
                default:
                    break;
            }

            throw new MQBrokerException(response.getCode(), response.getRemark());
        }
    }

    public ClusterInfo getBrokerClusterInfo(final long timeoutMillis) throws InterruptedException,
        RemotingTimeoutException, RemotingSendRequestException, RemotingConnectException, MQBrokerException {
        RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.GET_BROKER_CLUSTER_INFO, null);

        RemotingCommand response = this.remotingClient.invokeSync(null, request, timeoutMillis);
        assert response != null;
        switch (response.getCode()) {
            case ResponseCode.SUCCESS: {
                ClusterInfo responseBody = ClusterInfo.decode(response.getBody(), ClusterInfo.class);
                return responseBody;
            }
            default:
                break;
        }

        throw new MQBrokerException(response.getCode(), response.getRemark());

    }

    public TopicRouteData getDefaultTopicRouteInfoFromNameServer(final String topic, final long timeoutMillis)
        throws RemotingException, MQClientException, InterruptedException {
        GetRouteInfoRequestHeader requestHeader = new GetRouteInfoRequestHeader();
        requestHeader.setTopic(topic);

        RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.GET_ROUTEINTO_BY_TOPIC,
            requestHeader);

        RemotingCommand response = this.remotingClient.invokeSync(null, request, timeoutMillis);
        assert response != null;
        switch (response.getCode()) {
            case ResponseCode.TOPIC_NOT_EXIST: {
                // TODO LOG
                break;
            }
            case ResponseCode.SUCCESS: {
                byte[] body = response.getBody();
                if (body != null) {
                    return TopicRouteData.decode(body, TopicRouteData.class);
                }
            }
            default:
                break;
        }

        throw new MQClientException(response.getCode(), response.getRemark());
    }

    public TopicRouteData getTopicRouteInfoFromNameServer(final String topic, final long timeoutMillis)
        throws RemotingException, MQClientException, InterruptedException {
        String topicWithProjectGroup = topic;
        if (!UtilAll.isBlank(projectGroupPrefix)) {
            topicWithProjectGroup = VirtualEnvUtil.buildWithProjectGroup(topic, projectGroupPrefix);
        }

        GetRouteInfoRequestHeader requestHeader = new GetRouteInfoRequestHeader();
        requestHeader.setTopic(topicWithProjectGroup);

        RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.GET_ROUTEINTO_BY_TOPIC,
            requestHeader);

        RemotingCommand response = this.remotingClient.invokeSync(null, request, timeoutMillis);
        assert response != null;
        switch (response.getCode()) {
            case ResponseCode.TOPIC_NOT_EXIST: {
                log.warn("get Topic [{}] RouteInfoFromNameServer is not exist value", topic);
                break;
            }
            case ResponseCode.SUCCESS: {
                byte[] body = response.getBody();
                if (body != null) {
                    return TopicRouteData.decode(body, TopicRouteData.class);
                }
            }
            default:
                break;
        }

        throw new MQClientException(response.getCode(), response.getRemark());
    }

    public TopicList getTopicListFromNameServer(final long timeoutMillis) throws RemotingException, MQClientException,
        InterruptedException {
        RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.GET_ALL_TOPIC_LIST_FROM_NAMESERVER,
            null);

        RemotingCommand response = this.remotingClient.invokeSync(null, request, timeoutMillis);
        assert response != null;
        switch (response.getCode()) {
            case ResponseCode.SUCCESS: {
                byte[] body = response.getBody();
                if (body != null) {
                    TopicList topicList = TopicList.decode(body, TopicList.class);

                    if (!UtilAll.isBlank(projectGroupPrefix)) {
                        HashSet<String> newTopicSet = new HashSet<String>();
                        for (String topic : topicList.getTopicList()) {
                            newTopicSet.add(VirtualEnvUtil.clearProjectGroup(topic, projectGroupPrefix));
                        }
                        topicList.setTopicList(newTopicSet);
                    }
                    return topicList;
                }
            }
            default:
                break;
        }

        throw new MQClientException(response.getCode(), response.getRemark());
    }

    public int wipeWritePermOfBroker(final String namesrvAddr, String brokerName, final long timeoutMillis)
        throws RemotingCommandException, RemotingConnectException, RemotingSendRequestException,
        RemotingTimeoutException, InterruptedException, MQClientException {
        WipeWritePermOfBrokerRequestHeader requestHeader = new WipeWritePermOfBrokerRequestHeader();
        requestHeader.setBrokerName(brokerName);

        RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.WIPE_WRITE_PERM_OF_BROKER,
            requestHeader);

        RemotingCommand response = this.remotingClient.invokeSync(namesrvAddr, request, timeoutMillis);
        assert response != null;
        switch (response.getCode()) {
            case ResponseCode.SUCCESS: {
                WipeWritePermOfBrokerResponseHeader responseHeader = (WipeWritePermOfBrokerResponseHeader) response
                    .decodeCommandCustomHeader(WipeWritePermOfBrokerResponseHeader.class);
                return responseHeader.getWipeTopicCount();
            }
            default:
                break;
        }

        throw new MQClientException(response.getCode(), response.getRemark());
    }

    public void deleteTopicInBroker(final String addr, final String topic, final long timeoutMillis)
        throws RemotingException, MQBrokerException, InterruptedException, MQClientException {
        String topicWithProjectGroup = topic;
        if (!UtilAll.isBlank(projectGroupPrefix)) {
            topicWithProjectGroup = VirtualEnvUtil.buildWithProjectGroup(topic, projectGroupPrefix);
        }

        DeleteTopicRequestHeader requestHeader = new DeleteTopicRequestHeader();
        requestHeader.setTopic(topicWithProjectGroup);
        RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.DELETE_TOPIC_IN_BROKER,
            requestHeader);

        RemotingCommand response = this.remotingClient.invokeSync(addr, request, timeoutMillis);
        assert response != null;
        switch (response.getCode()) {
            case ResponseCode.SUCCESS: {
                return;
            }
            default:
                break;
        }

        throw new MQClientException(response.getCode(), response.getRemark());
    }

    public void deleteTopicInNameServer(final String addr, final String topic, final long timeoutMillis)
        throws RemotingException, MQBrokerException, InterruptedException, MQClientException {
        String topicWithProjectGroup = topic;
        if (!UtilAll.isBlank(projectGroupPrefix)) {
            topicWithProjectGroup = VirtualEnvUtil.buildWithProjectGroup(topic, projectGroupPrefix);
        }

        DeleteTopicRequestHeader requestHeader = new DeleteTopicRequestHeader();
        requestHeader.setTopic(topicWithProjectGroup);
        RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.DELETE_TOPIC_IN_NAMESRV,
            requestHeader);

        RemotingCommand response = this.remotingClient.invokeSync(addr, request, timeoutMillis);
        assert response != null;
        switch (response.getCode()) {
            case ResponseCode.SUCCESS: {
                return;
            }
            default:
                break;
        }

        throw new MQClientException(response.getCode(), response.getRemark());
    }

    public void deleteSubscriptionGroup(final String addr, final String groupName, final long timeoutMillis)
        throws RemotingException, MQBrokerException, InterruptedException, MQClientException {
        String groupWithProjectGroup = groupName;
        if (!UtilAll.isBlank(projectGroupPrefix)) {
            groupWithProjectGroup = VirtualEnvUtil.buildWithProjectGroup(groupName, projectGroupPrefix);
        }

        DeleteSubscriptionGroupRequestHeader requestHeader = new DeleteSubscriptionGroupRequestHeader();
        requestHeader.setGroupName(groupWithProjectGroup);
        RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.DELETE_SUBSCRIPTIONGROUP,
            requestHeader);

        RemotingCommand response = this.remotingClient.invokeSync(addr, request, timeoutMillis);
        assert response != null;
        switch (response.getCode()) {
            case ResponseCode.SUCCESS: {
                return;
            }
            default:
                break;
        }

        throw new MQClientException(response.getCode(), response.getRemark());
    }

    public void putKVConfigValue(final String namespace, final String key, final String value, final long timeoutMillis)
        throws RemotingException, MQClientException, InterruptedException {
        PutKVConfigRequestHeader requestHeader = new PutKVConfigRequestHeader();
        requestHeader.setNamespace(namespace);
        requestHeader.setKey(key);
        requestHeader.setValue(value);

        RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.PUT_KV_CONFIG, requestHeader);

        List<String> nameServerAddressList = this.remotingClient.getNameServerAddressList();
        if (nameServerAddressList != null) {
            RemotingCommand errResponse = null;
            for (String namesrvAddr : nameServerAddressList) {
                RemotingCommand response = this.remotingClient.invokeSync(namesrvAddr, request, timeoutMillis);
                assert response != null;
                switch (response.getCode()) {
                    case ResponseCode.SUCCESS: {
                        break;
                    }
                    default:
                        errResponse = response;
                }
            }

            if (errResponse != null) {
                throw new MQClientException(errResponse.getCode(), errResponse.getRemark());
            }
        }
    }

    public void deleteKVConfigValue(final String namespace, final String key, final long timeoutMillis)
        throws RemotingException, MQClientException, InterruptedException {
        DeleteKVConfigRequestHeader requestHeader = new DeleteKVConfigRequestHeader();
        requestHeader.setNamespace(namespace);
        requestHeader.setKey(key);

        RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.DELETE_KV_CONFIG, requestHeader);

        List<String> nameServerAddressList = this.remotingClient.getNameServerAddressList();
        if (nameServerAddressList != null) {
            RemotingCommand errResponse = null;
            for (String namesrvAddr : nameServerAddressList) {
                RemotingCommand response = this.remotingClient.invokeSync(namesrvAddr, request, timeoutMillis);
                assert response != null;
                switch (response.getCode()) {
                    case ResponseCode.SUCCESS: {
                        break;
                    }
                    default:
                        errResponse = response;
                }
            }
            if (errResponse != null) {
                throw new MQClientException(errResponse.getCode(), errResponse.getRemark());
            }
        }
    }

    public String getKVConfigByValue(final String namespace, String value, final long timeoutMillis)
        throws RemotingException, MQClientException, InterruptedException {
        GetKVConfigRequestHeader requestHeader = new GetKVConfigRequestHeader();
        requestHeader.setNamespace(namespace);
        requestHeader.setKey(value);

        RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.GET_KV_CONFIG_BY_VALUE,
            requestHeader);

        RemotingCommand response = this.remotingClient.invokeSync(null, request, timeoutMillis);
        assert response != null;
        switch (response.getCode()) {
            case ResponseCode.SUCCESS: {
                GetKVConfigResponseHeader responseHeader = (GetKVConfigResponseHeader) response
                    .decodeCommandCustomHeader(GetKVConfigResponseHeader.class);
                return responseHeader.getValue();
            }
            default:
                break;
        }

        throw new MQClientException(response.getCode(), response.getRemark());
    }

    public KVTable getKVListByNamespace(final String namespace, final long timeoutMillis) throws RemotingException,
        MQClientException, InterruptedException {
        GetKVListByNamespaceRequestHeader requestHeader = new GetKVListByNamespaceRequestHeader();
        requestHeader.setNamespace(namespace);

        RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.GET_KVLIST_BY_NAMESPACE,
            requestHeader);

        RemotingCommand response = this.remotingClient.invokeSync(null, request, timeoutMillis);
        assert response != null;
        switch (response.getCode()) {
            case ResponseCode.SUCCESS: {
                return KVTable.decode(response.getBody(), KVTable.class);
            }
            default:
                break;
        }

        throw new MQClientException(response.getCode(), response.getRemark());
    }

    public void deleteKVConfigByValue(final String namespace, final String projectGroup, final long timeoutMillis)
        throws RemotingException, MQClientException, InterruptedException {
        DeleteKVConfigRequestHeader requestHeader = new DeleteKVConfigRequestHeader();
        requestHeader.setNamespace(namespace);
        requestHeader.setKey(projectGroup);

        RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.DELETE_KV_CONFIG_BY_VALUE,
            requestHeader);

        List<String> nameServerAddressList = this.remotingClient.getNameServerAddressList();

        if (nameServerAddressList != null) {
            RemotingCommand errResponse = null;
            for (String namesrvAddr : nameServerAddressList) {
                RemotingCommand response = this.remotingClient.invokeSync(namesrvAddr, request, timeoutMillis);
                assert response != null;
                switch (response.getCode()) {
                    case ResponseCode.SUCCESS: {
                        break;
                    }
                    default:
                        errResponse = response;
                }
            }
            if (errResponse != null) {
                throw new MQClientException(errResponse.getCode(), errResponse.getRemark());
            }
        }
    }

    public Map<MessageQueue, Long> invokeBrokerToResetOffset(final String addr, final String topic, final String group,
        final long timestamp, final boolean isForce, final long timeoutMillis) throws RemotingException,
        MQClientException, InterruptedException {
        ResetOffsetRequestHeader requestHeader = new ResetOffsetRequestHeader();
        requestHeader.setTopic(topic);
        requestHeader.setGroup(group);
        requestHeader.setTimestamp(timestamp);
        requestHeader.setForce(isForce);

        RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.INVOKE_BROKER_TO_RESET_OFFSET,
            requestHeader);

        RemotingCommand response = this.remotingClient.invokeSync(addr, request, timeoutMillis);
        assert response != null;
        switch (response.getCode()) {
            case ResponseCode.SUCCESS: {
                if (response.getBody() != null) {
                    ResetOffsetBody body = ResetOffsetBody.decode(response.getBody(), ResetOffsetBody.class);
                    return body.getOffsetTable();
                }
            }
            default:
                break;
        }

        throw new MQClientException(response.getCode(), response.getRemark());
    }

    public Map<String, Map<MessageQueue, Long>> invokeBrokerToGetConsumerStatus(final String addr, final String topic,
        final String group, final String clientAddr, final long timeoutMillis) throws RemotingException,
        MQClientException, InterruptedException {
        GetConsumerStatusRequestHeader requestHeader = new GetConsumerStatusRequestHeader();
        requestHeader.setTopic(topic);
        requestHeader.setGroup(group);
        requestHeader.setClientAddr(clientAddr);

        RemotingCommand request = RemotingCommand.createRequestCommand(
            RequestCode.INVOKE_BROKER_TO_GET_CONSUMER_STATUS, requestHeader);

        RemotingCommand response = this.remotingClient.invokeSync(addr, request, timeoutMillis);
        assert response != null;
        switch (response.getCode()) {
            case ResponseCode.SUCCESS: {
                if (response.getBody() != null) {
                    GetConsumerStatusBody body = GetConsumerStatusBody.decode(response.getBody(),
                        GetConsumerStatusBody.class);
                    return body.getConsumerTable();
                }
            }
            default:
                break;
        }

        throw new MQClientException(response.getCode(), response.getRemark());
    }

    public GroupList queryTopicConsumeByWho(final String addr, final String topic, final long timeoutMillis)
        throws RemotingConnectException, RemotingSendRequestException, RemotingTimeoutException, InterruptedException,
        MQBrokerException {
        QueryTopicConsumeByWhoRequestHeader requestHeader = new QueryTopicConsumeByWhoRequestHeader();
        requestHeader.setTopic(topic);

        RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.QUERY_TOPIC_CONSUME_BY_WHO,
            requestHeader);

        RemotingCommand response = this.remotingClient.invokeSync(addr, request, timeoutMillis);
        switch (response.getCode()) {
            case ResponseCode.SUCCESS: {
                GroupList groupList = GroupList.decode(response.getBody(), GroupList.class);
                return groupList;
            }
            default:
                break;
        }

        throw new MQBrokerException(response.getCode(), response.getRemark());
    }

    public Set<QueueTimeSpan> queryConsumeTimeSpan(final String addr, final String topic, final String group,
        final long timeoutMillis) throws RemotingConnectException, RemotingSendRequestException,
        RemotingTimeoutException, InterruptedException, MQBrokerException {
        QueryConsumeTimeSpanRequestHeader requestHeader = new QueryConsumeTimeSpanRequestHeader();
        requestHeader.setTopic(topic);
        requestHeader.setGroup(group);

        RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.QUERY_CONSUME_TIME_SPAN,
            requestHeader);

        RemotingCommand response = this.remotingClient.invokeSync(addr, request, timeoutMillis);
        switch (response.getCode()) {
            case ResponseCode.SUCCESS: {
                QueryConsumeTimeSpanBody consumeTimeSpanBody = GroupList.decode(response.getBody(),
                    QueryConsumeTimeSpanBody.class);
                return consumeTimeSpanBody.getConsumeTimeSpanSet();
            }
            default:
                break;
        }

        throw new MQBrokerException(response.getCode(), response.getRemark());
    }

    public TopicList getTopicsByCluster(final String cluster, final long timeoutMillis) throws RemotingException,
        MQClientException, InterruptedException {
        GetTopicsByClusterRequestHeader requestHeader = new GetTopicsByClusterRequestHeader();
        requestHeader.setCluster(cluster);
        RemotingCommand request = RemotingCommand
            .createRequestCommand(RequestCode.GET_TOPICS_BY_CLUSTER, requestHeader);

        RemotingCommand response = this.remotingClient.invokeSync(null, request, timeoutMillis);
        assert response != null;
        switch (response.getCode()) {
            case ResponseCode.SUCCESS: {
                byte[] body = response.getBody();
                if (body != null) {
                    TopicList topicList = TopicList.decode(body, TopicList.class);

                    if (!UtilAll.isBlank(projectGroupPrefix)) {
                        HashSet<String> newTopicSet = new HashSet<String>();
                        for (String topic : topicList.getTopicList()) {
                            newTopicSet.add(VirtualEnvUtil.clearProjectGroup(topic, projectGroupPrefix));
                        }
                        topicList.setTopicList(newTopicSet);
                    }
                    return topicList;
                }
            }
            default:
                break;
        }

        throw new MQClientException(response.getCode(), response.getRemark());
    }

    public void registerMessageFilterClass(final String addr,//
        final String consumerGroup,//
        final String topic,//
        final String className,//
        final int classCRC,//
        final byte[] classBody,//
        final long timeoutMillis) throws RemotingConnectException, RemotingSendRequestException,
        RemotingTimeoutException, InterruptedException, MQBrokerException {
        RegisterMessageFilterClassRequestHeader requestHeader = new RegisterMessageFilterClassRequestHeader();
        requestHeader.setConsumerGroup(consumerGroup);
        requestHeader.setClassName(className);
        requestHeader.setTopic(topic);
        requestHeader.setClassCRC(classCRC);

        RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.REGISTER_MESSAGE_FILTER_CLASS,
            requestHeader);
        request.setBody(classBody);
        RemotingCommand response = this.remotingClient.invokeSync(addr, request, timeoutMillis);
        switch (response.getCode()) {
            case ResponseCode.SUCCESS: {
                return;
            }
            default:
                break;
        }

        throw new MQBrokerException(response.getCode(), response.getRemark());
    }

    public TopicList getSystemTopicList(final long timeoutMillis) throws RemotingException, MQClientException,
        InterruptedException {
        RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.GET_SYSTEM_TOPIC_LIST_FROM_NS, null);

        RemotingCommand response = this.remotingClient.invokeSync(null, request, timeoutMillis);
        assert response != null;
        switch (response.getCode()) {
            case ResponseCode.SUCCESS: {
                byte[] body = response.getBody();
                if (body != null) {
                    TopicList topicList = TopicList.decode(response.getBody(), TopicList.class);
                    if (!UtilAll.isBlank(projectGroupPrefix)) {
                        HashSet<String> newTopicSet = new HashSet<String>();
                        for (String topic : topicList.getTopicList()) {
                            newTopicSet.add(VirtualEnvUtil.clearProjectGroup(topic, projectGroupPrefix));
                        }
                        topicList.setTopicList(newTopicSet);
                    }

                    if (topicList.getTopicList() != null && !topicList.getTopicList().isEmpty()
                        && !UtilAll.isBlank(topicList.getBrokerAddr())) {
                        TopicList tmp = getSystemTopicListFromBroker(topicList.getBrokerAddr(), timeoutMillis);
                        if (tmp.getTopicList() != null && !tmp.getTopicList().isEmpty()) {
                            topicList.getTopicList().addAll(tmp.getTopicList());
                        }
                    }
                    return topicList;
                }
            }
            default:
                break;
        }

        throw new MQClientException(response.getCode(), response.getRemark());
    }

    public TopicList getSystemTopicListFromBroker(final String addr, final long timeoutMillis)
        throws RemotingException, MQClientException, InterruptedException {
        RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.GET_SYSTEM_TOPIC_LIST_FROM_BROKER,
            null);

        RemotingCommand response = this.remotingClient.invokeSync(addr, request, timeoutMillis);
        assert response != null;
        switch (response.getCode()) {
            case ResponseCode.SUCCESS: {
                byte[] body = response.getBody();
                if (body != null) {
                    TopicList topicList = TopicList.decode(body, TopicList.class);
                    if (!UtilAll.isBlank(projectGroupPrefix)) {
                        HashSet<String> newTopicSet = new HashSet<String>();
                        for (String topic : topicList.getTopicList()) {
                            newTopicSet.add(VirtualEnvUtil.clearProjectGroup(topic, projectGroupPrefix));
                        }
                        topicList.setTopicList(newTopicSet);
                    }
                    return topicList;
                }
            }
            default:
                break;
        }

        throw new MQClientException(response.getCode(), response.getRemark());
    }

    public boolean cleanExpiredConsumeQueue(final String addr, long timeoutMillis) throws MQClientException,
        RemotingConnectException, RemotingSendRequestException, RemotingTimeoutException, InterruptedException {
        RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.CLEAN_EXPIRED_CONSUMEQUEUE, null);
        RemotingCommand response = this.remotingClient.invokeSync(addr, request, timeoutMillis);
        switch (response.getCode()) {
            case ResponseCode.SUCCESS: {
                return true;
            }
            default:
                break;
        }

        throw new MQClientException(response.getCode(), response.getRemark());
    }

    public ConsumerRunningInfo getConsumerRunningInfo(final String addr, String consumerGroup, String clientId,
        boolean jstack, final long timeoutMillis) throws RemotingException, MQClientException, InterruptedException {
        GetConsumerRunningInfoRequestHeader requestHeader = new GetConsumerRunningInfoRequestHeader();
        requestHeader.setConsumerGroup(consumerGroup);
        requestHeader.setClientId(clientId);
        requestHeader.setJstackEnable(jstack);

        RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.GET_CONSUMER_RUNNING_INFO,
            requestHeader);

        RemotingCommand response = this.remotingClient.invokeSync(addr, request, timeoutMillis);
        assert response != null;
        switch (response.getCode()) {
            case ResponseCode.SUCCESS: {
                byte[] body = response.getBody();
                if (body != null) {
                    ConsumerRunningInfo info = ConsumerRunningInfo.decode(body, ConsumerRunningInfo.class);
                    return info;
                }
            }
            default:
                break;
        }

        throw new MQClientException(response.getCode(), response.getRemark());
    }

    public ConsumeMessageDirectlyResult consumeMessageDirectly(final String addr, //
        String consumerGroup, //
        String clientId, //
        String msgId, //
        final long timeoutMillis) throws RemotingException, MQClientException, InterruptedException {
        ConsumeMessageDirectlyResultRequestHeader requestHeader = new ConsumeMessageDirectlyResultRequestHeader();
        requestHeader.setConsumerGroup(consumerGroup);
        requestHeader.setClientId(clientId);
        requestHeader.setMsgId(msgId);

        RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.CONSUME_MESSAGE_DIRECTLY,
            requestHeader);

        RemotingCommand response = this.remotingClient.invokeSync(addr, request, timeoutMillis);
        assert response != null;
        switch (response.getCode()) {
            case ResponseCode.SUCCESS: {
                byte[] body = response.getBody();
                if (body != null) {
                    ConsumeMessageDirectlyResult info = ConsumeMessageDirectlyResult.decode(body,
                        ConsumeMessageDirectlyResult.class);
                    return info;
                }
            }
            default:
                break;
        }

        throw new MQClientException(response.getCode(), response.getRemark());
    }

    public String getProjectGroupPrefix() {
        return projectGroupPrefix;
    }

    public Map<Integer, Long> queryCorrectionOffset(final String addr, final String topic, final String group,
        Set<String> filterGroup, long timeoutMillis) throws MQClientException, RemotingConnectException,
        RemotingSendRequestException, RemotingTimeoutException, InterruptedException {
        QueryCorrectionOffsetHeader requestHeader = new QueryCorrectionOffsetHeader();
        requestHeader.setCompareGroup(group);
        requestHeader.setTopic(topic);
        if (filterGroup != null) {
            StringBuilder sb = new StringBuilder();
            String splitor = "";
            for (String s : filterGroup) {
                sb.append(splitor).append(s);
                splitor = ",";
            }
            requestHeader.setFilterGroups(sb.toString());
        }
        RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.QUERY_CORRECTION_OFFSET,
            requestHeader);

        RemotingCommand response = this.remotingClient.invokeSync(addr, request, timeoutMillis);
        assert response != null;
        switch (response.getCode()) {
            case ResponseCode.SUCCESS: {
                if (response.getBody() != null) {
                    QueryCorrectionOffsetBody body = QueryCorrectionOffsetBody.decode(response.getBody(),
                        QueryCorrectionOffsetBody.class);
                    return body.getCorrectionOffsets();
                }
            }
            default:
                break;
        }

        throw new MQClientException(response.getCode(), response.getRemark());
    }

    public TopicList getUnitTopicList(final boolean containRetry, final long timeoutMillis) throws RemotingException,
        MQClientException, InterruptedException {
        RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.GET_UNIT_TOPIC_LIST, null);

        RemotingCommand response = this.remotingClient.invokeSync(null, request, timeoutMillis);
        assert response != null;
        switch (response.getCode()) {
            case ResponseCode.SUCCESS: {
                byte[] body = response.getBody();
                if (body != null) {
                    TopicList topicList = TopicList.decode(response.getBody(), TopicList.class);
                    if (!UtilAll.isBlank(projectGroupPrefix)) {
                        HashSet<String> newTopicSet = new HashSet<String>();
                        for (String topic : topicList.getTopicList()) {
                            newTopicSet.add(VirtualEnvUtil.clearProjectGroup(topic, projectGroupPrefix));
                        }
                        topicList.setTopicList(newTopicSet);
                    }
                    if (!containRetry) {
                        Iterator<String> it = topicList.getTopicList().iterator();
                        while (it.hasNext()) {
                            String topic = it.next();
                            if (topic.startsWith(MixAll.RETRY_GROUP_TOPIC_PREFIX))
                                it.remove();
                        }
                    }

                    return topicList;
                }
            }
            default:
                break;
        }

        throw new MQClientException(response.getCode(), response.getRemark());
    }

    public TopicList getHasUnitSubTopicList(final boolean containRetry, final long timeoutMillis)
        throws RemotingException, MQClientException, InterruptedException {
        RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.GET_HAS_UNIT_SUB_TOPIC_LIST, null);

        RemotingCommand response = this.remotingClient.invokeSync(null, request, timeoutMillis);
        assert response != null;
        switch (response.getCode()) {
            case ResponseCode.SUCCESS: {
                byte[] body = response.getBody();
                if (body != null) {
                    TopicList topicList = TopicList.decode(response.getBody(), TopicList.class);
                    if (!UtilAll.isBlank(projectGroupPrefix)) {
                        HashSet<String> newTopicSet = new HashSet<String>();
                        for (String topic : topicList.getTopicList()) {
                            newTopicSet.add(VirtualEnvUtil.clearProjectGroup(topic, projectGroupPrefix));
                        }
                        topicList.setTopicList(newTopicSet);
                    }
                    if (!containRetry) {
                        Iterator<String> it = topicList.getTopicList().iterator();
                        while (it.hasNext()) {
                            String topic = it.next();
                            if (topic.startsWith(MixAll.RETRY_GROUP_TOPIC_PREFIX))
                                it.remove();
                        }
                    }
                    return topicList;
                }
            }
            default:
                break;
        }

        throw new MQClientException(response.getCode(), response.getRemark());
    }

    public TopicList getHasUnitSubUnUnitTopicList(final boolean containRetry, final long timeoutMillis)
        throws RemotingException, MQClientException, InterruptedException {
        RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.GET_HAS_UNIT_SUB_UNUNIT_TOPIC_LIST,
            null);

        RemotingCommand response = this.remotingClient.invokeSync(null, request, timeoutMillis);
        assert response != null;
        switch (response.getCode()) {
            case ResponseCode.SUCCESS: {
                byte[] body = response.getBody();
                if (body != null) {
                    TopicList topicList = TopicList.decode(response.getBody(), TopicList.class);
                    if (!UtilAll.isBlank(projectGroupPrefix)) {
                        HashSet<String> newTopicSet = new HashSet<String>();
                        for (String topic : topicList.getTopicList()) {
                            newTopicSet.add(VirtualEnvUtil.clearProjectGroup(topic, projectGroupPrefix));
                        }
                        topicList.setTopicList(newTopicSet);
                    }
                    if (!containRetry) {
                        Iterator<String> it = topicList.getTopicList().iterator();
                        while (it.hasNext()) {
                            String topic = it.next();
                            if (topic.startsWith(MixAll.RETRY_GROUP_TOPIC_PREFIX))
                                it.remove();
                        }
                    }
                    return topicList;
                }
            }
            default:
                break;
        }

        throw new MQClientException(response.getCode(), response.getRemark());
    }

    public void cloneGroupOffset(final String addr, final String srcGroup, final String destGroup, final String topic,
        final boolean isOffline, final long timeoutMillis) throws RemotingException, MQClientException,
        InterruptedException {
        CloneGroupOffsetRequestHeader requestHeader = new CloneGroupOffsetRequestHeader();
        requestHeader.setSrcGroup(srcGroup);
        requestHeader.setDestGroup(destGroup);
        requestHeader.setTopic(topic);
        requestHeader.setOffline(isOffline);
        RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.CLONE_GROUP_OFFSET, null);

        RemotingCommand response = this.remotingClient.invokeSync(addr, request, timeoutMillis);
        assert response != null;
        switch (response.getCode()) {
            case ResponseCode.SUCCESS: {
                return;
            }
            default:
                break;
        }

        throw new MQClientException(response.getCode(), response.getRemark());
    }

    public BrokerStatsData ViewBrokerStatsData(String brokerAddr, String statsName, String statsKey, long timeoutMillis)
        throws MQClientException, RemotingConnectException, RemotingSendRequestException, RemotingTimeoutException,
        InterruptedException {
        ViewBrokerStatsDataRequestHeader requestHeader = new ViewBrokerStatsDataRequestHeader();
        requestHeader.setStatsName(statsName);
        requestHeader.setStatsKey(statsKey);

        RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.VIEW_BROKER_STATS_DATA,
            requestHeader);

        RemotingCommand response = this.remotingClient.invokeSync(brokerAddr, request, timeoutMillis);
        assert response != null;
        switch (response.getCode()) {
            case ResponseCode.SUCCESS: {
                byte[] body = response.getBody();
                if (body != null) {
                    return BrokerStatsData.decode(body, BrokerStatsData.class);
                }
            }
            default:
                break;
        }

        throw new MQClientException(response.getCode(), response.getRemark());
    }

}

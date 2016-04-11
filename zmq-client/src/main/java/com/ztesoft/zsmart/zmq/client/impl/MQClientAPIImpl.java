package com.ztesoft.zsmart.zmq.client.impl;

import java.util.ArrayList;
import java.util.List;

import org.slf4j.Logger;

import com.ztesoft.zsmart.zmq.client.VirtualEnvUtil;
import com.ztesoft.zsmart.zmq.client.exception.MQBrokerException;
import com.ztesoft.zsmart.zmq.client.exception.MQClientException;
import com.ztesoft.zsmart.zmq.client.log.ClientLogger;
import com.ztesoft.zsmart.zmq.client.producer.SendCallback;
import com.ztesoft.zsmart.zmq.client.producer.SendResult;
import com.ztesoft.zsmart.zmq.client.producer.SendStatus;
import com.ztesoft.zsmart.zmq.common.MQVersion;
import com.ztesoft.zsmart.zmq.common.MixAll;
import com.ztesoft.zsmart.zmq.common.TopicConfig;
import com.ztesoft.zsmart.zmq.common.UtilAll;
import com.ztesoft.zsmart.zmq.common.message.Message;
import com.ztesoft.zsmart.zmq.common.message.MessageQueue;
import com.ztesoft.zsmart.zmq.common.namesrv.NamesrvUtil;
import com.ztesoft.zsmart.zmq.common.namesrv.TopAddressing;
import com.ztesoft.zsmart.zmq.common.protocol.RequestCode;
import com.ztesoft.zsmart.zmq.common.protocol.ResponseCode;
import com.ztesoft.zsmart.zmq.common.protocol.header.CreateTopicRequestHeader;
import com.ztesoft.zsmart.zmq.common.protocol.header.SendMessageRequestHeader;
import com.ztesoft.zsmart.zmq.common.protocol.header.SendMessageRequestHeaderV2;
import com.ztesoft.zsmart.zmq.common.protocol.header.SendMessageResponseHeader;
import com.ztesoft.zsmart.zmq.common.protocol.header.namesrv.GetKVConfigRequestHeader;
import com.ztesoft.zsmart.zmq.common.protocol.header.namesrv.GetKVConfigResponseHeader;
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

}

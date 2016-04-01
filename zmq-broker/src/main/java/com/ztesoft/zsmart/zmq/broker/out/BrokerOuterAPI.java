package com.ztesoft.zsmart.zmq.broker.out;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.ztesoft.zsmart.zmq.client.exception.MQBrokerException;
import com.ztesoft.zsmart.zmq.common.MixAll;
import com.ztesoft.zsmart.zmq.common.constant.LoggerName;
import com.ztesoft.zsmart.zmq.common.namesrv.RegisterBrokerResult;
import com.ztesoft.zsmart.zmq.common.namesrv.TopAddressing;
import com.ztesoft.zsmart.zmq.common.protocol.RequestCode;
import com.ztesoft.zsmart.zmq.common.protocol.ResponseCode;
import com.ztesoft.zsmart.zmq.common.protocol.body.KVTable;
import com.ztesoft.zsmart.zmq.common.protocol.body.RegisterBrokerBody;
import com.ztesoft.zsmart.zmq.common.protocol.body.SubscriptionGroupWrapper;
import com.ztesoft.zsmart.zmq.common.protocol.body.TopicConfigSerializeWrapper;
import com.ztesoft.zsmart.zmq.common.protocol.header.namesrv.RegisterBrokerRequestHeader;
import com.ztesoft.zsmart.zmq.common.protocol.header.namesrv.RegisterBrokerResponseHeader;
import com.ztesoft.zsmart.zmq.remoting.RPCHook;
import com.ztesoft.zsmart.zmq.remoting.RemotingClient;
import com.ztesoft.zsmart.zmq.remoting.exception.RemotingCommandException;
import com.ztesoft.zsmart.zmq.remoting.exception.RemotingConnectException;
import com.ztesoft.zsmart.zmq.remoting.exception.RemotingSendRequestException;
import com.ztesoft.zsmart.zmq.remoting.exception.RemotingTimeoutException;
import com.ztesoft.zsmart.zmq.remoting.exception.RemotingTooMuchRequestException;
import com.ztesoft.zsmart.zmq.remoting.netty.NettyClientConfig;
import com.ztesoft.zsmart.zmq.remoting.netty.NettyRemotingClient;
import com.ztesoft.zsmart.zmq.remoting.protocol.RemotingCommand;
import com.ztesoft.zsmart.zmq.remoting.protocol.RemotingSerializable;

/**
 * Broker对外调用的API封装 <br>
 * 
 * @author wang.jun<br>
 * @version 1.0<br>
 * @taskId <br>
 * @CreateDate 2016年4月1日 <br>
 * @since V7.3<br>
 * @see com.ztesoft.zsmart.zmq.broker.out <br>
 */
public class BrokerOuterAPI {
    private static final Logger log = LoggerFactory.getLogger(LoggerName.BrokerLoggerName);

    private final RemotingClient remotingClient;

    private final TopAddressing topAddressing = new TopAddressing(MixAll.WS_ADDR);

    private String nameSrvAddr = null;

    public BrokerOuterAPI(final NettyClientConfig nettyClientConfig, RPCHook hook) {
        this.remotingClient = new NettyRemotingClient(nettyClientConfig);
        this.remotingClient.registerRPCHook(hook);
    }

    public BrokerOuterAPI(final NettyClientConfig nettyClientConfig) {
        this(nettyClientConfig, null);
    }

    public void start() {
        this.remotingClient.start();
    }

    public void shutdown() {
        this.remotingClient.shutdown();
    }

    /**
     * http://jmenv.ztesoft.com:8080/zmq/nsaddr 加载namesrv配置: <br>
     * 
     * @author wang.jun<br>
     * @taskId <br>
     * @return <br>
     */
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

    /**
     * 更新namesrv 地址: <br>
     * 
     * @author wang.jun<br>
     * @taskId <br>
     * @param addrs <br>
     */
    public void updateNameServerAddressList(String addrs) {
        List<String> lst = new ArrayList<String>();
        String[] addrArray = addrs.split(";");
        if (addrArray != null) {
            lst.addAll(Arrays.asList(addrArray));

            this.remotingClient.updateNameServerAddressList(lst);
        }
    }

    /**
     * 在指定namesrv 上注册 broker: <br>
     * 
     * @author wang.jun<br>
     * @taskId <br>
     * @param namesrvAddr
     * @param clusterName
     * @param brokerAddr
     * @param brokerName
     * @param brokerId
     * @param haServerAddr
     * @param topicConfigWrapper
     * @param filterServerList
     * @param oneway
     * @return
     * @throws RemotingConnectException
     * @throws RemotingTimeoutException
     * @throws RemotingSendRequestException
     * @throws InterruptedException
     * @throws RemotingCommandException
     * @throws MQBrokerException <br>
     */
    private RegisterBrokerResult registerBroker(//
        final String namesrvAddr,//
        final String clusterName,//
        final String brokerAddr,//
        final String brokerName,//
        final long brokerId,//
        final String haServerAddr,//
        final TopicConfigSerializeWrapper topicConfigWrapper,//
        final List<String> filterServerList,//
        final boolean oneway) throws RemotingConnectException, RemotingTimeoutException, RemotingSendRequestException,
        InterruptedException, RemotingCommandException, MQBrokerException {
        RegisterBrokerRequestHeader requestHeader = new RegisterBrokerRequestHeader();
        requestHeader.setBrokerAddr(brokerAddr);
        requestHeader.setBrokerId(brokerId);
        requestHeader.setBrokerName(brokerName);
        requestHeader.setClusterName(clusterName);
        requestHeader.setHaServerAddr(haServerAddr);

        RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.REGISTER_BROKER, requestHeader);

        RegisterBrokerBody requestBody = new RegisterBrokerBody();
        requestBody.setTopicConfigSerializeWrapper(topicConfigWrapper);
        requestBody.setFilterServerList(filterServerList);
        request.setBody(requestBody.encode());

        if (oneway) {
            try {
                this.remotingClient.invokeOneway(namesrvAddr, request, 3000);
            }
            catch (RemotingTooMuchRequestException e) {
            }
            return null;
        }

        RemotingCommand response = this.remotingClient.invokeSync(namesrvAddr, request, 3000);

        assert response != null;

        switch (response.getCode()) {
            case ResponseCode.SUCCESS:
                RegisterBrokerResponseHeader responseHeader = (RegisterBrokerResponseHeader) response
                    .decodeCommandCustomHeader(RegisterBrokerResponseHeader.class);

                RegisterBrokerResult result = new RegisterBrokerResult();
                result.setMasterAddr(responseHeader.getMasterAddr());
                result.setHaServerAddr(responseHeader.getHaServerAddr());

                if (response.getBody() != null) {
                    result.setKvTable(RemotingSerializable.decode(response.getBody(), KVTable.class));
                }
                return result;
        }

        throw new MQBrokerException(response.getCode(), response.getRemark());
    }

    /**
     * 在所有 namesrv上注册 broker: <br>
     * 
     * @author wang.jun<br>
     * @taskId <br>
     * @param namesrvAddr
     * @param clusterName
     * @param brokerAddr
     * @param brokerName
     * @param brokerId
     * @param haServerAddr
     * @param topicConfigWrapper
     * @param filterServerList
     * @param oneway
     * @return <br>
     */
    public RegisterBrokerResult registerBrokerAll(//
        final String clusterName,//
        final String brokerAddr,//
        final String brokerName,//
        final long brokerId,//
        final String haServerAddr,//
        final TopicConfigSerializeWrapper topicConfigWrapper,//
        final List<String> filterServerList,//
        final boolean oneway) {
        RegisterBrokerResult registerBrokerResult = null;

        List<String> nameServerAddressList = this.remotingClient.getNameServerAddressList();
        if (nameServerAddressList != null) {
            for (String namesrvAddr : nameServerAddressList) {
                try {
                    registerBrokerResult = this.registerBroker(namesrvAddr, clusterName, brokerAddr, brokerName,
                        brokerId, haServerAddr, topicConfigWrapper, filterServerList, oneway);
                    log.info("register broker to name server {} OK", namesrvAddr);
                }
                catch (Exception e) {
                    log.warn("registerBroker Exception, " + namesrvAddr, e);
                }
            }
        }
        return registerBrokerResult;
    }

    /**
     * 从Master 同步所有topic config: <br>
     * 
     * @author wang.jun<br>
     * @taskId <br>
     * @param addr
     * @return <br>
     * @throws InterruptedException
     * @throws RemotingTimeoutException
     * @throws RemotingSendRequestException
     * @throws RemotingConnectException
     * @throws MQBrokerException
     */
    public TopicConfigSerializeWrapper getAllTopicConfig(final String addr) throws RemotingConnectException,
        RemotingSendRequestException, RemotingTimeoutException, InterruptedException, MQBrokerException {
        RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.GET_ALL_TOPIC_CONFIG, null);

        RemotingCommand response = this.remotingClient.invokeSync(addr, request, 3000);

        assert response != null;

        switch (response.getCode()) {
            case ResponseCode.SUCCESS:
                return TopicConfigSerializeWrapper.decode(response.getBody(), TopicConfigSerializeWrapper.class);
        }

        throw new MQBrokerException(response.getCode(), response.getRemark());
    }

    /**
     * 获取订阅组配置: <br>
     * 
     * @author wang.jun<br>
     * @taskId <br>
     * @param addr
     * @return
     * @throws RemotingSendRequestException
     * @throws RemotingTimeoutException
     * @throws InterruptedException
     * @throws MQBrokerException
     * @throws RemotingConnectException <br>
     */
    public SubscriptionGroupWrapper getAllSubscriptionGroupConfig(final String addr)
        throws RemotingSendRequestException, RemotingTimeoutException, InterruptedException, MQBrokerException,
        RemotingConnectException {
        RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.GET_ALL_SUBSCRIPTIONGROUP_CONFIG,
            null);

        RemotingCommand response = this.remotingClient.invokeSync(addr, request, 3000);

        assert response != null;

        switch (response.getCode()) {
            case ResponseCode.SUCCESS:
                return SubscriptionGroupWrapper.decode(response.getBody(), SubscriptionGroupWrapper.class);
        }

        throw new MQBrokerException(response.getCode(), response.getRemark());

    }

    /**
     * 获取所有定时进度: <br>
     * 
     * @author wang.jun<br>
     * @taskId <br>
     * @param addr
     * @return <br>
     * @throws InterruptedException 
     * @throws RemotingTimeoutException 
     * @throws RemotingSendRequestException 
     * @throws RemotingConnectException 
     * @throws MQBrokerException 
     */
    public String getAllDelayOffset(final String addr) throws RemotingConnectException, RemotingSendRequestException,
        RemotingTimeoutException, InterruptedException, MQBrokerException {
        RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.GET_ALL_DELAY_OFFSET, null);
        RemotingCommand response = this.remotingClient.invokeSync(addr, request, 3000);
        assert response != null;
        switch (response.getCode()) {
            case ResponseCode.SUCCESS: {
                return new String(response.getBody());
            }
            default:
                break;
        }

        throw new MQBrokerException(response.getCode(), response.getRemark());

    }

}

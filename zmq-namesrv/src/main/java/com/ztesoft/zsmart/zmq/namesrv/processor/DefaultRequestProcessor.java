package com.ztesoft.zsmart.zmq.namesrv.processor;

import java.util.concurrent.atomic.AtomicLong;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.netty.channel.ChannelHandlerContext;

import com.ztesoft.zsmart.zmq.common.MQVersion;
import com.ztesoft.zsmart.zmq.common.MQVersion.Version;
import com.ztesoft.zsmart.zmq.common.constant.LoggerName;
import com.ztesoft.zsmart.zmq.common.namesrv.NamesrvUtil;
import com.ztesoft.zsmart.zmq.common.namesrv.RegisterBrokerResult;
import com.ztesoft.zsmart.zmq.common.protocol.RequestCode;
import com.ztesoft.zsmart.zmq.common.protocol.ResponseCode;
import com.ztesoft.zsmart.zmq.common.protocol.body.RegisterBrokerBody;
import com.ztesoft.zsmart.zmq.common.protocol.body.TopicConfigSerializeWrapper;
import com.ztesoft.zsmart.zmq.common.protocol.header.namesrv.DeleteKVConfigRequestHeader;
import com.ztesoft.zsmart.zmq.common.protocol.header.namesrv.GetKVConfigRequestHeader;
import com.ztesoft.zsmart.zmq.common.protocol.header.namesrv.GetKVConfigResponseHeader;
import com.ztesoft.zsmart.zmq.common.protocol.header.namesrv.GetRouteInfoRequestHeader;
import com.ztesoft.zsmart.zmq.common.protocol.header.namesrv.PutKVConfigRequestHeader;
import com.ztesoft.zsmart.zmq.common.protocol.header.namesrv.RegisterBrokerRequestHeader;
import com.ztesoft.zsmart.zmq.common.protocol.header.namesrv.RegisterBrokerResponseHeader;
import com.ztesoft.zsmart.zmq.common.protocol.header.namesrv.UnRegisterBrokerRequestHeader;
import com.ztesoft.zsmart.zmq.common.protocol.header.namesrv.WipeWritePermOfBrokerRequestHeader;
import com.ztesoft.zsmart.zmq.common.protocol.header.namesrv.WipeWritePermOfBrokerResponseHeader;
import com.ztesoft.zsmart.zmq.common.protocol.route.TopicRouteData;
import com.ztesoft.zsmart.zmq.namesrv.NamesrvController;
import com.ztesoft.zsmart.zmq.remoting.common.RemotingHelper;
import com.ztesoft.zsmart.zmq.remoting.exception.RemotingCommandException;
import com.ztesoft.zsmart.zmq.remoting.netty.NettyRequestProcessor;
import com.ztesoft.zsmart.zmq.remoting.protocol.RemotingCommand;
import com.ztesoft.zsmart.zmq.remoting.protocol.RemotingSerializable;

public class DefaultRequestProcessor implements NettyRequestProcessor {
    private static final Logger log = LoggerFactory.getLogger(LoggerName.NamesrvLoggerName);

    private final NamesrvController namesrvController;

    public DefaultRequestProcessor(NamesrvController namesrvController) {
        this.namesrvController = namesrvController;
    }

    @Override
    public RemotingCommand processRequest(ChannelHandlerContext ctx, RemotingCommand request) throws Exception {

        if (log.isDebugEnabled()) {
            log.debug("receive request {} {} {}", request.getCode(),
                RemotingHelper.parseChannelRemoteAddr(ctx.channel()), request);
        }

        switch (request.getCode()) {
            case RequestCode.PUT_KV_CONFIG: // 配置kv
                return this.putKVConfig(ctx, request);
            case RequestCode.GET_KV_CONFIG:
                return this.getKVConfig(ctx, request); // 获取kv配置
            case RequestCode.DELETE_KV_CONFIG:
                return this.deleteKVConfig(ctx, request);// 删除kv配置
            case RequestCode.REGISTER_BROKER: // 注册borker
                Version brokerVersion = MQVersion.value2Version(request.getVersion());
                // 新版本Broker，支持Filter Server
                if (brokerVersion.ordinal() >= MQVersion.Version.V3_2_6.ordinal()) {
                    return this.registerBrokerWithFilterServer(ctx, request);
                }// 低版本Broker，不支持Filter Server
                else {
                    return this.registerBroker(ctx, request);
                }
            case RequestCode.UNREGISTER_BROKER:
                return this.unregisterBroker(ctx, request); // Namesrv 卸载一个Broker，数据都是持久化的
            case RequestCode.GET_ROUTEINTO_BY_TOPIC: // Namesrv 根据Topic获取Broker Name、队列数(包含读队列与写队列)
                return this.getRouteInfoByTopic(ctx, request);
            case RequestCode.GET_BROKER_CLUSTER_INFO:
                return this.getBrokerClusterInfo(ctx, request); // Namesrv 获取注册到Name Server的所有Broker集群信息
            case RequestCode.WIPE_WRITE_PERM_OF_BROKER:
                return this.wipeWritePermOfBroker(ctx, request);
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
    private RemotingCommand wipeWritePermOfBroker(ChannelHandlerContext ctx, RemotingCommand request)
        throws RemotingCommandException {
        final RemotingCommand response = RemotingCommand
            .createResponseCommand(WipeWritePermOfBrokerResponseHeader.class);
        final WipeWritePermOfBrokerResponseHeader responseHeader = (WipeWritePermOfBrokerResponseHeader) response
            .readCustomHeader();
        final WipeWritePermOfBrokerRequestHeader requestHeader = (WipeWritePermOfBrokerRequestHeader) request
            .decodeCommandCustomHeader(WipeWritePermOfBrokerRequestHeader.class);

        int wipeTopicCnt = this.namesrvController.getRouteInfoManager().wipeWritePermOfBrokerByLock(
            requestHeader.getBrokerName());

        log.info("wipe write perm of broker[{}], client: {}, {}", //
            requestHeader.getBrokerName(), //
            RemotingHelper.parseChannelRemoteAddr(ctx.channel()), //
            wipeTopicCnt);

        responseHeader.setWipeTopicCount(wipeTopicCnt);
        response.setCode(ResponseCode.SUCCESS);
        response.setRemark(null);
        return response;
    }

    /**
     * // Namesrv 获取注册到Name Server的所有Broker集群信息: <br>
     * 
     * @author wang.jun<br>
     * @taskId <br>
     * @param ctx
     * @param request
     * @return <br>
     */
    private RemotingCommand getBrokerClusterInfo(ChannelHandlerContext ctx, RemotingCommand request) {
        final RemotingCommand response = RemotingCommand.createResponseCommand(null);

        byte[] content = this.namesrvController.getRouteInfoManager().getAllClusterInfo();
        response.setBody(content);
        response.setCode(ResponseCode.SUCCESS);
        response.setRemark(null);
        return response;
    }

    /**
     * Namesrv 根据Topic获取Broker Name、队列数(包含读队列与写队列): <br>
     * 
     * @author wang.jun<br>
     * @taskId <br>
     * @param ctx
     * @param request
     * @return <br>
     * @throws RemotingCommandException
     */
    private RemotingCommand getRouteInfoByTopic(ChannelHandlerContext ctx, RemotingCommand request)
        throws RemotingCommandException {
        final RemotingCommand response = RemotingCommand.createResponseCommand(null);
        final GetRouteInfoRequestHeader requestHeader = (GetRouteInfoRequestHeader) request
            .decodeCommandCustomHeader(GetRouteInfoRequestHeader.class);

        TopicRouteData topicRouteData = this.namesrvController.getRouteInfoManager().pickupTopicRouteData(
            requestHeader.getTopic());

        if (topicRouteData != null) {
            String orderTopicConf = this.namesrvController.getKvConfigManager().getKVConfig(
                NamesrvUtil.NAMESPACE_ORDER_TOPIC_CONFIG, requestHeader.getTopic());

            topicRouteData.setOrderTopicConf(orderTopicConf);
            byte[] content = topicRouteData.encode();
            response.setBody(content);
            response.setCode(ResponseCode.SUCCESS);
            response.setRemark(null);
        }

        return response;
    }

    /**
     * 从Namesrv 卸载broker: <br>
     * 
     * @author wang.jun<br>
     * @taskId <br>
     * @param ctx
     * @param request
     * @return <br>
     * @throws RemotingCommandException
     */
    private RemotingCommand unregisterBroker(ChannelHandlerContext ctx, RemotingCommand request)
        throws RemotingCommandException {
        final RemotingCommand response = RemotingCommand.createResponseCommand(null);
        final UnRegisterBrokerRequestHeader requestHeader = (UnRegisterBrokerRequestHeader) request
            .decodeCommandCustomHeader(UnRegisterBrokerRequestHeader.class);

        this.namesrvController.getRouteInfoManager().unregisterBroker(//
            requestHeader.getClusterName(),//
            requestHeader.getBrokerAddr(),//
            requestHeader.getBrokerName(),//
            requestHeader.getBrokerId());
        response.setCode(ResponseCode.SUCCESS);
        response.setRemark(null);

        return response;
    }

    /**
     * 在NameSrv注册 broker: <br>
     * 
     * @author wang.jun<br>
     * @taskId <br>
     * @param ctx
     * @param request
     * @return
     * @throws RemotingCommandException <br>
     */
    private RemotingCommand registerBroker(ChannelHandlerContext ctx, RemotingCommand request)
        throws RemotingCommandException {
        final RemotingCommand response = RemotingCommand.createResponseCommand(RegisterBrokerResponseHeader.class);
        final RegisterBrokerResponseHeader responseHeader = (RegisterBrokerResponseHeader) response.readCustomHeader();
        final RegisterBrokerRequestHeader requestHeader = (RegisterBrokerRequestHeader) request
            .decodeCommandCustomHeader(RegisterBrokerRequestHeader.class);

        TopicConfigSerializeWrapper topicConfigWrapper = null;
        if (request.getBody() != null) {
            topicConfigWrapper = TopicConfigSerializeWrapper.decode(request.getBody(),
                TopicConfigSerializeWrapper.class);
        }
        else {
            topicConfigWrapper = new TopicConfigSerializeWrapper();
            topicConfigWrapper.getDataVersion().setCounter(new AtomicLong(0));
            topicConfigWrapper.getDataVersion().setTimestatmp(0);
        }

        RegisterBrokerResult result = this.namesrvController.getRouteInfoManager().registerBroker(//
            requestHeader.getClusterName(), // 1
            requestHeader.getBrokerAddr(), // 2
            requestHeader.getBrokerName(), // 3
            requestHeader.getBrokerId(), // 4
            requestHeader.getHaServerAddr(),// 5
            topicConfigWrapper, // 6
            null,//
            ctx.channel()// 7
            );

        responseHeader.setHaServerAddr(result.getHaServerAddr());
        responseHeader.setMasterAddr(result.getMasterAddr());

        // 获取顺序消息 topic 列表
        byte[] jsonValue = this.namesrvController.getKvConfigManager().getKVListByNamespace(
            NamesrvUtil.NAMESPACE_ORDER_TOPIC_CONFIG);
        response.setBody(jsonValue);
        response.setCode(ResponseCode.SUCCESS);
        response.setRemark(null);
        return response;
    }

    /**
     * 注册filter broker: <br>
     * 
     * @author wang.jun<br>
     * @taskId <br>
     * @param ctx
     * @param request
     * @return <br>
     * @throws RemotingCommandException
     */
    private RemotingCommand registerBrokerWithFilterServer(ChannelHandlerContext ctx, RemotingCommand request)
        throws RemotingCommandException {
        final RemotingCommand response = RemotingCommand.createResponseCommand(RegisterBrokerResponseHeader.class);

        final RegisterBrokerResponseHeader responseHeader = (RegisterBrokerResponseHeader) response.readCustomHeader();

        final RegisterBrokerRequestHeader requestHeader = (RegisterBrokerRequestHeader) request
            .decodeCommandCustomHeader(RegisterBrokerRequestHeader.class);

        RegisterBrokerBody registerBrokerBody = new RegisterBrokerBody();

        if (request.getBody() != null) {
            registerBrokerBody = RemotingSerializable.decode(request.getBody(), RegisterBrokerBody.class);
        }
        else {
            registerBrokerBody.getTopicConfigSerializeWrapper().getDataVersion().setCounter(new AtomicLong(0));
            registerBrokerBody.getTopicConfigSerializeWrapper().getDataVersion().setTimestatmp(0);
        }

        RegisterBrokerResult result = this.namesrvController.getRouteInfoManager().registerBroker(
            requestHeader.getClusterName(), //
            requestHeader.getBrokerAddr(), //
            requestHeader.getBrokerName(),//
            requestHeader.getBrokerId(), //
            requestHeader.getHaServerAddr(),//
            registerBrokerBody.getTopicConfigSerializeWrapper(), //
            registerBrokerBody.getFilterServerList(),//
            ctx.channel());

        responseHeader.setHaServerAddr(result.getHaServerAddr());
        responseHeader.setMasterAddr(result.getMasterAddr());
        // 获取顺序消息 topic 列表
        byte[] jsonValue = this.namesrvController.getKvConfigManager().getKVListByNamespace(
            NamesrvUtil.NAMESPACE_ORDER_TOPIC_CONFIG);

        response.setBody(jsonValue);
        response.setCode(ResponseCode.SUCCESS);
        response.setRemark(null);

        return response;
    }

    /**
     * 删除kv配置: <br>
     * 
     * @author wang.jun<br>
     * @taskId <br>
     * @param ctx
     * @param request
     * @return <br>
     * @throws RemotingCommandException
     */
    private RemotingCommand deleteKVConfig(ChannelHandlerContext ctx, RemotingCommand request)
        throws RemotingCommandException {
        final RemotingCommand response = RemotingCommand.createResponseCommand(null);
        final DeleteKVConfigRequestHeader requestHeader = (DeleteKVConfigRequestHeader) request
            .decodeCommandCustomHeader(DeleteKVConfigRequestHeader.class);
        this.namesrvController.getKvConfigManager()
            .deleteKVConfig(requestHeader.getNamespace(), requestHeader.getKey());
        response.setCode(ResponseCode.SUCCESS);
        response.setRemark(null);
        return response;
    }

    /**
     * 获取kv配置: <br>
     * 
     * @author wang.jun<br>
     * @taskId <br>
     * @param ctx
     * @param request
     * @return
     * @throws RemotingCommandException <br>
     */
    private RemotingCommand getKVConfig(ChannelHandlerContext ctx, RemotingCommand request)
        throws RemotingCommandException {
        final RemotingCommand response = RemotingCommand.createResponseCommand(GetKVConfigResponseHeader.class);

        final GetKVConfigResponseHeader responseHeader = (GetKVConfigResponseHeader) response.readCustomHeader();

        final GetKVConfigRequestHeader requestHeader = (GetKVConfigRequestHeader) request
            .decodeCommandCustomHeader(GetKVConfigRequestHeader.class);

        String value = namesrvController.getKvConfigManager().getKVConfig(requestHeader.getNamespace(),
            requestHeader.getKey());

        if (value != null) {
            responseHeader.setValue(value);
            response.setCode(ResponseCode.SUCCESS);
            response.setRemark(null);
            return response;
        }
        response.setCode(ResponseCode.QUERY_NOT_FOUND);
        response.setRemark("No config item, Namespace: " + requestHeader.getNamespace() + " Key: "
            + requestHeader.getKey());
        return response;
    }

    /**
     * 设置kv配置: <br>
     * 
     * @author wang.jun<br>
     * @taskId <br>
     * @param ctx
     * @param request
     * @return
     * @throws RemotingCommandException <br>
     */
    private RemotingCommand putKVConfig(ChannelHandlerContext ctx, RemotingCommand request)
        throws RemotingCommandException {
        final RemotingCommand response = RemotingCommand.createResponseCommand(null);

        final PutKVConfigRequestHeader requestHeader = (PutKVConfigRequestHeader) request
            .decodeCommandCustomHeader(PutKVConfigRequestHeader.class);
        this.namesrvController.getKvConfigManager().putKVConfig(requestHeader.getNamespace(), requestHeader.getKey(),
            requestHeader.getValue());

        response.setCode(ResponseCode.SUCCESS);
        response.setRemark(null);
        return response;
    }

}

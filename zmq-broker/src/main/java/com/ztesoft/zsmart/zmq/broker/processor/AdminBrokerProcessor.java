/**
 * Copyright (C) 2010-2013 Alibaba Group Holding Limited
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.ztesoft.zsmart.zmq.broker.processor;

import java.io.UnsupportedEncodingException;
import java.net.UnknownHostException;
import java.sql.Connection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.ztesoft.zsmart.zmq.broker.BrokerController;
import com.ztesoft.zsmart.zmq.broker.client.ClientChannelInfo;
import com.ztesoft.zsmart.zmq.broker.client.ConsumerGroupInfo;
import com.ztesoft.zsmart.zmq.common.MQVersion;
import com.ztesoft.zsmart.zmq.common.MixAll;
import com.ztesoft.zsmart.zmq.common.TopicConfig;
import com.ztesoft.zsmart.zmq.common.UtilAll;
import com.ztesoft.zsmart.zmq.common.admin.ConsumeStats;
import com.ztesoft.zsmart.zmq.common.admin.OffsetWrapper;
import com.ztesoft.zsmart.zmq.common.admin.TopicOffset;
import com.ztesoft.zsmart.zmq.common.admin.TopicStatsTable;
import com.ztesoft.zsmart.zmq.common.constant.LoggerName;
import com.ztesoft.zsmart.zmq.common.message.MessageDecoder;
import com.ztesoft.zsmart.zmq.common.message.MessageId;
import com.ztesoft.zsmart.zmq.common.message.MessageQueue;
import com.ztesoft.zsmart.zmq.common.protocol.RequestCode;
import com.ztesoft.zsmart.zmq.common.protocol.ResponseCode;
import com.ztesoft.zsmart.zmq.common.protocol.body.KVTable;
import com.ztesoft.zsmart.zmq.common.protocol.body.TopicList;
import com.ztesoft.zsmart.zmq.common.protocol.filtersrv.RegisterFilterServerRequestHeader;
import com.ztesoft.zsmart.zmq.common.protocol.filtersrv.RegisterFilterServerResponseHeader;
import com.ztesoft.zsmart.zmq.common.protocol.header.GetConsumerStatusRequestHeader;
import com.ztesoft.zsmart.zmq.common.protocol.header.ResetOffsetRequestHeader;
import com.ztesoft.zsmart.zmq.common.protocol.heartbeat.SubscriptionData;
import com.ztesoft.zsmart.zmq.common.stats.StatsItem;
import com.ztesoft.zsmart.zmq.common.stats.StatsSnapshot;
import com.ztesoft.zsmart.zmq.common.subscription.SubscriptionGroupConfig;
import com.ztesoft.zsmart.zmq.remoting.common.RemotingHelper;
import com.ztesoft.zsmart.zmq.remoting.exception.RemotingCommandException;
import com.ztesoft.zsmart.zmq.remoting.exception.RemotingTimeoutException;
import com.ztesoft.zsmart.zmq.remoting.netty.NettyRequestProcessor;
import com.ztesoft.zsmart.zmq.remoting.protocol.RemotingCommand;
import com.ztesoft.zsmart.zmq.remoting.protocol.RemotingSerializable;
import com.ztesoft.zsmart.zmq.store.DefaultMessageStore;
import com.ztesoft.zsmart.zmq.store.SelectMapedBufferResult;

import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;


/**
 * 管理类请求处理
 * 
 * @author shijia.wxr<vintage.wang@gmail.com>
 * @author manhong.yqd<manhong.yqd@taobao.com>
 * @since 2013-7-26
 */
public class AdminBrokerProcessor implements NettyRequestProcessor {
    private static final Logger log = LoggerFactory.getLogger(LoggerName.BrokerLoggerName);
    private final BrokerController brokerController;


    public AdminBrokerProcessor(final BrokerController brokerController) {
        this.brokerController = brokerController;
    }


    @Override
    public RemotingCommand processRequest(ChannelHandlerContext ctx, RemotingCommand request)
            throws RemotingCommandException {
        switch (request.getCode()) {
//            // 更新创建Topic
//        case RequestCode.UPDATE_AND_CREATE_TOPIC:
//            return this.updateAndCreateTopic(ctx, request);
//            // 删除Topic
//        case RequestCode.DELETE_TOPIC_IN_BROKER:
//            return this.deleteTopic(ctx, request);
//            // 获取Topic配置
//        case RequestCode.GET_ALL_TOPIC_CONFIG:
//            return this.getAllTopicConfig(ctx, request);
//
//            // 更新Broker配置 TODO 可能存在并发问题
//        case RequestCode.UPDATE_BROKER_CONFIG:
//            return this.updateBrokerConfig(ctx, request);
//            // 获取Broker配置
//        case RequestCode.GET_BROKER_CONFIG:
//            return this.getBrokerConfig(ctx, request);
//
//            // 根据时间查询Offset
//        case RequestCode.SEARCH_OFFSET_BY_TIMESTAMP:
//            return this.searchOffsetByTimestamp(ctx, request);
//        case RequestCode.GET_MAX_OFFSET:
//            return this.getMaxOffset(ctx, request);
//        case RequestCode.GET_MIN_OFFSET:
//            return this.getMinOffset(ctx, request);
//        case RequestCode.GET_EARLIEST_MSG_STORETIME:
//            return this.getEarliestMsgStoretime(ctx, request);
//
//            // 获取Broker运行时信息
//        case RequestCode.GET_BROKER_RUNTIME_INFO:
//            return this.getBrokerRuntimeInfo(ctx, request);
//
//            // 锁队列与解锁队列
//        case RequestCode.LOCK_BATCH_MQ:
//            return this.lockBatchMQ(ctx, request);
//        case RequestCode.UNLOCK_BATCH_MQ:
//            return this.unlockBatchMQ(ctx, request);
//
//            // 订阅组配置
//        case RequestCode.UPDATE_AND_CREATE_SUBSCRIPTIONGROUP:
//            return this.updateAndCreateSubscriptionGroup(ctx, request);
//        case RequestCode.GET_ALL_SUBSCRIPTIONGROUP_CONFIG:
//            return this.getAllSubscriptionGroup(ctx, request);
//        case RequestCode.DELETE_SUBSCRIPTIONGROUP:
//            return this.deleteSubscriptionGroup(ctx, request);
//
//            // 统计信息，获取Topic统计信息
//        case RequestCode.GET_TOPIC_STATS_INFO:
//            return this.getTopicStatsInfo(ctx, request);
//
//            // Consumer连接管理
//        case RequestCode.GET_CONSUMER_CONNECTION_LIST:
//            return this.getConsumerConnectionList(ctx, request);
//            // Producer连接管理
//        case RequestCode.GET_PRODUCER_CONNECTION_LIST:
//            return this.getProducerConnectionList(ctx, request);
//
//            // 查询消费进度，订阅组下的所有Topic
//        case RequestCode.GET_CONSUME_STATS:
//            return this.getConsumeStats(ctx, request);
//        case RequestCode.GET_ALL_CONSUMER_OFFSET:
//            return this.getAllConsumerOffset(ctx, request);
//
//            // 定时进度
//        case RequestCode.GET_ALL_DELAY_OFFSET:
//            return this.getAllDelayOffset(ctx, request);
//
//            // 调用客户端重置 offset
//        case RequestCode.INVOKE_BROKER_TO_RESET_OFFSET:
//            return this.resetOffset(ctx, request);
//
//            // 调用客户端订阅消息处理
//        case RequestCode.INVOKE_BROKER_TO_GET_CONSUMER_STATUS:
//            return this.getConsumerStatus(ctx, request);
//
//            // 查询Topic被哪些消费者消费
//        case RequestCode.QUERY_TOPIC_CONSUME_BY_WHO:
//            return this.queryTopicConsumeByWho(ctx, request);
//
//        case RequestCode.REGISTER_FILTER_SERVER:
//            return this.registerFilterServer(ctx, request);
//            // 根据 topic 和 group 获取消息的时间跨度
//        case RequestCode.QUERY_CONSUME_TIME_SPAN:
//            return this.queryConsumeTimeSpan(ctx, request);
//        case RequestCode.GET_SYSTEM_TOPIC_LIST_FROM_BROKER:
//            return this.getSystemTopicListFromBroker(ctx, request);
//
//            // 删除失效队列
//        case RequestCode.CLEAN_EXPIRED_CONSUMEQUEUE:
//            return this.cleanExpiredConsumeQueue();
//
//        case RequestCode.GET_CONSUMER_RUNNING_INFO:
//            return this.getConsumerRunningInfo(ctx, request);
//
//            // 查找被修正 offset (转发组件）
//        case RequestCode.QUERY_CORRECTION_OFFSET:
//            return this.queryCorrectionOffset(ctx, request);
//
//        case RequestCode.CONSUME_MESSAGE_DIRECTLY:
//            return this.consumeMessageDirectly(ctx, request);
//        case RequestCode.CLONE_GROUP_OFFSET:
//            return this.cloneGroupOffset(ctx, request);
//
//            // 查看Broker统计信息
//        case RequestCode.VIEW_BROKER_STATS_DATA:
//            return ViewBrokerStatsData(ctx, request);
        default:
            break;
        }

        return null;
    }

 
}

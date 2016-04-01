package com.ztesoft.zsmart.zmq.broker.subscription;

import java.util.Iterator;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.ztesoft.zsmart.zmq.broker.BrokerController;
import com.ztesoft.zsmart.zmq.broker.BrokerPathConfigHelper;
import com.ztesoft.zsmart.zmq.common.ConfigManager;
import com.ztesoft.zsmart.zmq.common.DataVersion;
import com.ztesoft.zsmart.zmq.common.MixAll;
import com.ztesoft.zsmart.zmq.common.constant.LoggerName;
import com.ztesoft.zsmart.zmq.common.subscription.SubscriptionGroupConfig;
import com.ztesoft.zsmart.zmq.remoting.protocol.RemotingSerializable;

/**
 * 用来管理 订阅级 包括订阅权限等 <br>
 * 
 * @author wang.jun<br>
 * @version 1.0<br>
 * @taskId <br>
 * @CreateDate 2016年4月1日 <br>
 * @since V7.3<br>
 * @see com.ztesoft.zsmart.zmq.broker <br>
 */
public class SubscriptionGroupManager extends ConfigManager {
    private static final Logger log = LoggerFactory.getLogger(LoggerName.BrokerLoggerName);

    private transient BrokerController brokerController;

    // 订阅组
    private final ConcurrentHashMap<String, SubscriptionGroupConfig> subscriptionGroupTable = new ConcurrentHashMap<String, SubscriptionGroupConfig>(
        1024);

    private final DataVersion dataVersion = new DataVersion();

    public SubscriptionGroupManager() {
        this.init();
    }

    /**
     * 初始化: <br>
     * TOOLS_CONSUMER FILTERSRV_CONSUMER SELF_TEST_C_GROUP 订阅组
     * 
     * @author wang.jun<br>
     * @taskId <br>
     * <br>
     */
    private void init() {

        // TOOLS_CONSUMER
        SubscriptionGroupConfig subscriptionGroupConfig = new SubscriptionGroupConfig();
        subscriptionGroupConfig.setGroupName(MixAll.TOOLS_CONSUMER_GROUP);
        this.subscriptionGroupTable.put(MixAll.TOOLS_CONSUMER_GROUP, subscriptionGroupConfig);

        // FILTERSRV_CONSUMER
        subscriptionGroupConfig = new SubscriptionGroupConfig();
        subscriptionGroupConfig.setGroupName(MixAll.FILTERSRV_CONSUMER_GROUP);
        this.subscriptionGroupTable.put(MixAll.FILTERSRV_CONSUMER_GROUP, subscriptionGroupConfig);

        // SELF_TEST_C_GROUP
        subscriptionGroupConfig = new SubscriptionGroupConfig();
        subscriptionGroupConfig.setGroupName(MixAll.SELF_TEST_CONSUMER_GROUP);
        this.subscriptionGroupTable.put(MixAll.SELF_TEST_CONSUMER_GROUP, subscriptionGroupConfig);

    }

    public SubscriptionGroupManager(BrokerController brokerController) {
        this.brokerController = brokerController;
        this.init();
    }

    /**
     * 更新订阅组信息: <br>
     * 
     * @author wang.jun<br>
     * @taskId <br>
     * @param config <br>
     */
    public void updateSubscriptionGroupConfig(final SubscriptionGroupConfig config) {
        SubscriptionGroupConfig old = this.subscriptionGroupTable.put(config.getGroupName(), config);

        if (old != null) {
            log.info("update subscription group config, old: " + old + " new: " + config);
        }
        else {
            log.info("create new subscription group, " + config);
        }

        this.dataVersion.nextVersion();

        this.persist();
    }

    /**
     * 查找订阅组: <br>
     * 
     * @author wang.jun<br>
     * @taskId <br>
     * @param group
     * @return <br>
     */
    public SubscriptionGroupConfig findSubscriptionGroupConfig(final String group) {
        SubscriptionGroupConfig config = this.subscriptionGroupTable.get(group);
        if (null == subscriptionGroupTable) {
            if (brokerController.getBrokerConfig().isAutoCreateSubscriptionGroup()) {
                config = new SubscriptionGroupConfig();
                config.setGroupName(group);
                this.subscriptionGroupTable.put(group, config);
                log.info("auto create a subscription group, {}", config);
                this.dataVersion.nextVersion();
                this.persist();
            }
        }

        return config;
    }

    @Override
    public String encode() {
        return this.encode(false);
    }

    @Override
    public String encode(boolean prettyFormat) {
        return RemotingSerializable.toJson(this, prettyFormat);
    }

    @Override
    public void decode(String jsonString) {
        if (jsonString != null) {
            SubscriptionGroupManager obj = RemotingSerializable.fromJson(jsonString, SubscriptionGroupManager.class);
            if (obj != null) {
                this.subscriptionGroupTable.putAll(obj.subscriptionGroupTable);
                this.dataVersion.assignNewOne(obj.dataVersion);
                this.printLoadDataWhenFirstBoot(obj);
            }
        }
    }

    /**
     * 删除订阅组: <br>
     * 
     * @author wang.jun<br>
     * @taskId <br>
     * @param groupName <br>
     */
    public void deleteSubscriptionGroupConfig(final String groupName) {
        SubscriptionGroupConfig old = this.subscriptionGroupTable.remove(groupName);
        if (old != null) {
            log.info("delete subscription group OK, subscription group: " + old);
            this.dataVersion.nextVersion();
            this.persist();
        }
        else {
            log.warn("delete subscription group failed, subscription group: " + old + " not exist");
        }
    }

    private void printLoadDataWhenFirstBoot(final SubscriptionGroupManager sgm) {
        Iterator<Entry<String, SubscriptionGroupConfig>> it = sgm.getSubscriptionGroupTable().entrySet().iterator();
        while (it.hasNext()) {
            Entry<String, SubscriptionGroupConfig> next = it.next();
            log.info("load exist subscription group, {}", next.getValue().toString());
        }
    }

    @Override
    public String configFilePath() {
        return BrokerPathConfigHelper.getSubscriptionGroupPath(this.brokerController.getMessageStoreConfig()
            .getStorePathRootDir());
    }

    public BrokerController getBrokerController() {
        return brokerController;
    }

    public void setBrokerController(BrokerController brokerController) {
        this.brokerController = brokerController;
    }

    public ConcurrentHashMap<String, SubscriptionGroupConfig> getSubscriptionGroupTable() {
        return subscriptionGroupTable;
    }

    public DataVersion getDataVersion() {
        return dataVersion;
    }

}

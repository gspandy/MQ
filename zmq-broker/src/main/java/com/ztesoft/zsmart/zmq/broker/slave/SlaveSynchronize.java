package com.ztesoft.zsmart.zmq.broker.slave;

import java.io.IOException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.ztesoft.zsmart.zmq.broker.BrokerController;
import com.ztesoft.zsmart.zmq.common.MixAll;
import com.ztesoft.zsmart.zmq.common.constant.LoggerName;
import com.ztesoft.zsmart.zmq.common.protocol.body.ConsumerOffsetSerializeWrapper;
import com.ztesoft.zsmart.zmq.common.protocol.body.SubscriptionGroupWrapper;
import com.ztesoft.zsmart.zmq.common.protocol.body.TopicConfigSerializeWrapper;
import com.ztesoft.zsmart.zmq.store.config.StorePathConfigHelper;


/**
 * Slave从Master同步信息 <br>
 * 
 * @author wang.jun<br>
 * @version 1.0<br>
 * @taskId <br>
 * @CreateDate 2016年4月1日 <br>
 * @since V7.3<br>
 * @see com.ztesoft.zsmart.zmq.broker.slave <br>
 */
public class SlaveSynchronize {
    private static final Logger log = LoggerFactory.getLogger(LoggerName.BrokerLoggerName);

    private final BrokerController brokerController;

    private volatile String masterAddr = null;


    public SlaveSynchronize(BrokerController brokerController) {
        this.brokerController = brokerController;
    }


    public String getMasterAddr() {
        return masterAddr;
    }


    public void setMasterAddr(String masterAddr) {
        this.masterAddr = masterAddr;
    }


    public BrokerController getBrokerController() {
        return brokerController;
    }


    /**
     * 同步TopicConfig ConsumerOffset DelayOffset SubscriptionGroupConfig: <br>
     * 
     * @author wang.jun<br>
     * @taskId <br>
     *         <br>
     */
    public void syncAll() {
        syncTopicConfig();
        syncConsumerOffset(); // 同步消费进度
        syncSubscriptionGroupConfig();
        syncDelayOffset();// 同步时定消费进度
    }


    /**
     * 
     * 同步消费进度: <br>
     * 
     * @author wang.jun<br>
     * @taskId <br>
     *         <br>
     */
    private void syncConsumerOffset() {
        String masterAddrBak = this.masterAddr;

        if (masterAddrBak != null) {
            try {
                ConsumerOffsetSerializeWrapper offsetWrapper =
                        this.getBrokerController().getBrokerOuterAPI().getAllConsumerOffset(masterAddrBak);
                this.brokerController.getConsumerOffsetManager().getOffsetTable()
                    .putAll(offsetWrapper.getOffsetTable());
                this.brokerController.getConsumerOffsetManager().persist();
                log.info("update slave consumer offset from master, {}", masterAddrBak);
            }
            catch (Exception e) {
                log.error("syncConsumerOffset Exception, " + masterAddrBak, e);
            }
        }
    }


    /**
     * 从Master 同步topic: <br>
     * 
     * @author wang.jun<br>
     * @taskId <br>
     *         <br>
     */
    private void syncTopicConfig() {
        String masterAddrBak = this.masterAddr;

        if (masterAddrBak != null) {
            try {
                TopicConfigSerializeWrapper wrapper =
                        this.brokerController.getBrokerOuterAPI().getAllTopicConfig(masterAddrBak);

                if (!this.brokerController.getTopicConfigManager().getDataVersion()
                    .equals(wrapper.getDataVersion())) {
                    this.brokerController.getTopicConfigManager().getDataVersion()
                        .assignNewOne(wrapper.getDataVersion());
                    this.brokerController.getTopicConfigManager().getTopicConfigTable().clear();
                    this.brokerController.getTopicConfigManager().getTopicConfigTable()
                        .putAll(wrapper.getTopicConfigTable());

                    this.brokerController.getTopicConfigManager().persist();
                    log.info("update slave topic config from master, {}", masterAddrBak);

                }

            }
            catch (Exception e) {
                log.error("syncTopicConfig Exception, " + masterAddrBak, e);

            }
        }
    }


    /**
     * 同步订阅组信息: <br>
     * 
     * @author wang.jun<br>
     * @taskId <br>
     *         <br>
     */
    private void syncSubscriptionGroupConfig() {
        String masterAddrBak = this.masterAddr;

        if (masterAddrBak != null) {
            try {
                SubscriptionGroupWrapper wrapper = this.brokerController.getBrokerOuterAPI()
                    .getAllSubscriptionGroupConfig(masterAddrBak);

                if (!this.brokerController.getSubscriptionGroupManager().getDataVersion()
                    .equals(wrapper.getDataVersion())) {
                    this.brokerController.getSubscriptionGroupManager().getDataVersion()
                        .assignNewOne(wrapper.getDataVersion());
                    this.brokerController.getSubscriptionGroupManager().getSubscriptionGroupTable().clear();
                    this.brokerController.getSubscriptionGroupManager().getSubscriptionGroupTable()
                        .putAll(wrapper.getSubscriptionGroupTable());

                    this.brokerController.getSubscriptionGroupManager().persist();
                    log.info("update slave Subscription config from master, {}", masterAddrBak);

                }

            }
            catch (Exception e) {
                log.error("syncSubscriptionGroupConfig Exception, " + masterAddrBak, e);

            }
        }
    }


    /**
     * 获取所有定时进度: <br>
     * 
     * @author wang.jun<br>
     * @taskId <br>
     *         <br>
     */
    private void syncDelayOffset() {
        String masterAddrBak = this.masterAddr;

        if (masterAddrBak != null) {
            try {

                String delayOffset =
                        this.brokerController.getBrokerOuterAPI().getAllDelayOffset(masterAddrBak);

                if (delayOffset != null) {
                    String fileName = StorePathConfigHelper.getDelayOffsetStorePath(
                        this.brokerController.getMessageStoreConfig().getStorePathRootDir());

                    try {
                        MixAll.string2File(delayOffset, fileName);
                    }
                    catch (IOException e) {
                        log.error("persist file Exception, " + fileName, e);
                    }
                }
                log.info("update slave delay offset from master, {}", masterAddrBak);

            }
            catch (Exception e) {
                log.error("syncSubscriptionGroupConfig Exception, " + masterAddrBak, e);

            }
        }
    }

}

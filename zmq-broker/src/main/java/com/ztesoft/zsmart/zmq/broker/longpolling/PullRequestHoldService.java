package com.ztesoft.zsmart.zmq.broker.longpolling;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.ztesoft.zsmart.zmq.broker.BrokerController;
import com.ztesoft.zsmart.zmq.common.ServiceThread;
import com.ztesoft.zsmart.zmq.common.constant.LoggerName;
import com.ztesoft.zsmart.zmq.remoting.exception.RemotingCommandException;


/**
 * 拉消息请求管理，如果拉不到消息，则在这里Hold住，等待消息到来
 * 
 * @author J.Wang
 *
 */
public class PullRequestHoldService extends ServiceThread {
    private static final Logger log = LoggerFactory.getLogger(LoggerName.BrokerLoggerName);
    private static final String TOPIC_QUEUEID_SEPARATOR = "@";

    private ConcurrentHashMap<String/* topic@queueId */, ManyPullRequest> pullRequestTable =
            new ConcurrentHashMap<String, ManyPullRequest>(1024);

    private final BrokerController brokerController;


    public PullRequestHoldService(final BrokerController brokerController) {
        this.brokerController = brokerController;
    }


    /**
     * 构建topic@queueId
     * 
     * @param topic
     * @param queueId
     * @return
     */
    private String buildKey(final String topic, final int queueId) {
        return topic + TOPIC_QUEUEID_SEPARATOR + queueId;
    }


    public void suspendPullRequest(final String topic, final int queueId, final PullRequest pullRequest) {
        String key = this.buildKey(topic, queueId);
        ManyPullRequest mpr = this.pullRequestTable.get(key);
        if (null == mpr) {
            mpr = new ManyPullRequest();
            ManyPullRequest prev = this.pullRequestTable.putIfAbsent(key, mpr);
            if (prev != null) {
                mpr = prev;
            }
        }

        mpr.addPullRequest(pullRequest);
    }


    private void checkHoldRequest() {
        for (String key : this.pullRequestTable.keySet()) {
            String[] kArray = key.split(TOPIC_QUEUEID_SEPARATOR);
            if (kArray != null && 2 == kArray.length) {
                String topic = kArray[0];
                int queueId = Integer.parseInt(kArray[1]);
                final long offset =
                        this.brokerController.getMessageStore().getMaxOffsetInQuque(topic, queueId);
                this.notifyMessageArriving(topic, queueId, offset);
            }
        }
    }


    /**
     * 通知message
     * 
     * @param topic
     * @param queueId
     * @param offset
     */
    public void notifyMessageArriving(String topic, int queueId, long maxOffset) {
        String key = this.buildKey(topic, queueId);
        ManyPullRequest mpr = this.pullRequestTable.get(key);
        if (mpr != null) {
            List<PullRequest> requestList = mpr.cloneListAndClear();
            if (requestList != null) {
                List<PullRequest> replayList = new ArrayList<PullRequest>();

                for (PullRequest request : requestList) {
                    // 查看是否offset ok
                    if (maxOffset > request.getPullFromThisOffset()) {
                        try {
                            this.brokerController.getPullMessageProcessor().excuteRequestWhenWakeup(
                                request.getClientChannel(), request.getRequestCommand());
                        }
                        catch (RemotingCommandException e) {
                            log.error(null, e);
                        }
                        continue;
                    }
                    else { // 尝试取最新Offset
                        final long newestOffset =
                                this.brokerController.getMessageStore().getMaxOffsetInQuque(topic, queueId);
                        if (newestOffset > request.getPullFromThisOffset()) {
                            try {
                                this.brokerController.getPullMessageProcessor().excuteRequestWhenWakeup(
                                    request.getClientChannel(), request.getRequestCommand());
                            }
                            catch (RemotingCommandException e) {
                                log.error("", e);
                            }
                            continue;
                        }
                    }

                    // 查看是否超时
                    if (System.currentTimeMillis() >= (request.getSuspendTimestamp()
                            + request.getTimeoutMillis())) {
                        try {
                            this.brokerController.getPullMessageProcessor().excuteRequestWhenWakeup(
                                request.getClientChannel(), request.getRequestCommand());
                        }
                        catch (RemotingCommandException e) {
                            log.error("", e);
                        }
                        continue;
                    }

                    // 当前不满足要求，重新放回Hold列表中
                    replayList.add(request);
                }

                if (!replayList.isEmpty()) {
                    mpr.addPullRequest(replayList);
                }
            }
        }

    }


    @Override
    public void run() {
        log.info(this.getServiceName() + " service started");
        while (!this.isStoped()) {
            try {
                this.waitForRunning(1000);
                this.checkHoldRequest();
            }
            catch (Exception e) {
                log.warn(this.getServiceName() + " service has exception. ", e);
            }
        }

        log.info(this.getServiceName() + " service end");

    }


    @Override
    public String getServiceName() {
        return PullRequestHoldService.class.getSimpleName();
    }

}

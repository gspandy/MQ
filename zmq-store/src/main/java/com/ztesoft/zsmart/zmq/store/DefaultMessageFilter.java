package com.ztesoft.zsmart.zmq.store;

import com.ztesoft.zsmart.zmq.common.protocol.heartbeat.SubscriptionData;

/**
 * 消息过滤规则实现 <br>
 * 
 * @author wang.jun<br>
 * @version 1.0<br>
 * @taskId <br>
 * @CreateDate 2016年3月24日 <br>
 * @since V7.3<br>
 * @see com.ztesoft.zsmart.zmq.store <br>
 */
public class DefaultMessageFilter implements MessageFilter {

    @Override
    public boolean isMessageMatched(SubscriptionData subscriptionData, long tagsCode) {
        if (null == subscriptionData) {
            return true;
        }

        if (subscriptionData.isClassFilterMode()) {
            return true;
        }

        if (subscriptionData.getSubString().equals(SubscriptionData.SUB_ALL)) {
            return true;
        }

        return subscriptionData.getCodeSet().contains((int) tagsCode);
    }

}

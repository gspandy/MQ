package com.ztesoft.zsmart.zmq.broker.processor;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.ztesoft.zsmart.zmq.broker.BrokerController;
import com.ztesoft.zsmart.zmq.common.constant.LoggerName;

/**
 * 拉消息请求处理 <br>
 * 
 * @author wang.jun<br>
 * @version 1.0<br>
 * @taskId <br>
 * @CreateDate 2016年4月5日 <br>
 * @since V7.3<br>
 * @see com.ztesoft.zsmart.zmq.broker <br>
 */
public class PullMessageProcessor {
    private static final Logger log = LoggerFactory.getLogger(LoggerName.BrokerLoggerName);

    private final BrokerController brokerController;

    public PullMessageProcessor(final BrokerController brokerController) {
        this.brokerController = brokerController;
    }

}

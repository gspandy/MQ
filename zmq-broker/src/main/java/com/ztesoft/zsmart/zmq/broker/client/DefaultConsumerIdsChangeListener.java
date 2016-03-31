package com.ztesoft.zsmart.zmq.broker.client;

import java.util.List;

import io.netty.channel.Channel;

import com.ztesoft.zsmart.zmq.broker.BrokerController;

/**
 * 
 * ConsumerId列表变化，通知所有Consumer <br> 
 *  
 * @author wang.jun<br>
 * @version 1.0<br>
 * @taskId <br>
 * @CreateDate 2016年3月31日 <br>
 * @since V7.3<br>
 * @see com.ztesoft.zsmart.zmq.broker.client <br>
 */
public class DefaultConsumerIdsChangeListener implements ConsumerIdsChangeListener {

    private final BrokerController brokerController;


    public DefaultConsumerIdsChangeListener(BrokerController brokerController) {
        this.brokerController = brokerController;
    }
    
    @Override
    public void consumerIdsChanged(String group, List<Channel> channels) {
        if(channels != null && brokerController.getBrokerConfig().isNotifyConsumerIdsChangedEnable()){
            for(Channel chl : channels){
                 
            }
        }
        
    }

}
